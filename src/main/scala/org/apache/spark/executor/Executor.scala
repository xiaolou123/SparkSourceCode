/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import akka.actor.Props

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader,
  SparkUncaughtExceptionHandler, AkkaUtils, Utils}

/**
 * Spark executor used with Mesos, YARN, and the standalone scheduler.
 * In coarse-grained mode, an existing actor system is provided.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false)
  extends Logging
{

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  @volatile private var isStopped = false

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  // Start worker thread pool
  val threadPool = Utils.newDaemonCachedThreadPool("Executor task launch worker")

  val executorSource = new ExecutorSource(this, executorId)

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Create an actor for receiving RPCs from the driver
  private val executorActor = env.actorSystem.actorOf(
    Props(new ExecutorActor(executorId)), "ExecutorActor")

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst: Boolean = {
    conf.getBoolean("spark.executor.userClassPathFirst",
      conf.getBoolean("spark.files.userClassPathFirst", false))
  }

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(urlClassLoader)

  // Akka's message frame size. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

  // Limit of bytes for total size of results (default is 1GB)
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  startDriverHeartbeater()

  def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer) {
    // 对于每个task，都会创建一个taskRunner
    // TaskRunner继承的是Java的多线程中的Runnable接口
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    // 将TaskRunner放入内存缓存
    runningTasks.put(taskId, tr)
    // 又来了，java，ThreadPool，Executor内部有过一个java线程池
    // 然后呢，这里其实将task封装在一个线程中(TaskRunner)
    // 直接将线程丢入线程池，进行执行
    // 所以这里我们要知道，线程池是自动实现了排队机制的，也就是说，如果线程池的线程暂时没有空闲的
    // 那么丢进来的线程都是要排队的
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }

  def stop() {
    env.metricsSystem.report()
    env.actorSystem.stop(executorActor)
    isStopped = true
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  private def gcTime = ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum

  /**
   * 从TaskRunner开始，就是我们的Task运行的工作原理
   * ok，准备一步一步来剖析Task内部的工作原理
   */
  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskId: Long,
      val attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer)
    extends Runnable {

    @volatile private var killed = false
    @volatile var task: Task[Any] = _
    @volatile var attemptedTask: Option[Task[Any]] = None
    @volatile var startGCTime: Long = _

    def kill(interruptThread: Boolean) {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId)")
      killed = true
      if (task != null) {
        task.kill(interruptThread)
      }
    }

    override def run() {
      val deserializeStartTime = System.currentTimeMillis()
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      startGCTime = gcTime

      try {
        // 对序列化的task数据，进行反序列化
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        // 然后，通过网络通信，将需要的文件、资源、jar拷贝过来
        updateDependencies(taskFiles, taskJars)
        // 最后，通过正式的反序列化操作，将整个task的数据集反序列化回来
        // 为什么要用到java的ClassLader
        // 因为java的ClassLoader，可以干很多事情
        // 比如，用反射的方式来动态加载一个类，然后创建这个类的对象
        // 还有，比如，可以用来对指定上下文的相关资源，进行加载和读取
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        attemptedTask = Some(task)
        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        // 计算出task开始的时间
        taskStart = System.currentTimeMillis()
        // 执行task！！！
        // 用的是Task的run()方法
        // 这里的value，对于ShuffleMapTask来说，其实就是MapStatus
        // 封装了ShuffleMapTask计算的数据，输出的位置
        // 后面，如果还是一个ShuffleMapTask呢？
        // 那么就会去联系MapOutputTracker，来获取上一个ShuffleMapTask的输出位置，然后通过网络拉取数据
        // ResultTask也是一样的
        val value = task.run(taskAttemptId = taskId, attemptNumber = attemptNumber)
        // 计算出task结束的时间
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        // 这个，其实就是对MapStatus进行了各种序列化，和封装，因为后面要发送给Driver（通过网络）
        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        // 这儿，是计算出task相关的一些metrics，就是统计信息
        // 那么包括运行了多长时间，反序列化耗费了多长时间，java虚拟机gc耗费了多长时间，结果的序列化耗费了多长时间
        // 这些东西，其实会在我们的SparkUI上显示，那么大家真正在企业中运行我们的长时间运行的spark程序时
        //肯定会自己到4040端口，去观察sparkUI
        for (m <- task.metrics) {
          m.setExecutorDeserializeTime(taskStart - deserializeStartTime)
          m.setExecutorRunTime(taskFinish - taskStart)
          m.setJvmGCTime(gcTime - startGCTime)
          m.setResultSerializationTime(afterSerialization - beforeSerialization)
        }

        val accumUpdates = Accumulators.values

        val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        // 这个非常核心，其实就是调用了Executor所在的CoraseGrainedExecutorBackend的statusUpdate()方法
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException => {
          val reason = ffe.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }

        case _: TaskKilledException | _: InterruptedException if task.killed => {
          logInfo(s"Executor killed $taskName (TID $taskId)")
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))
        }

        case cDE: CommitDeniedException => {
          val reason = cDE.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
        }

        case t: Throwable => {
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          val serviceTime = System.currentTimeMillis() - taskStart
          val metrics = attemptedTask.flatMap(t => t.metrics)
          for (m <- metrics) {
            m.setExecutorRunTime(serviceTime)
            m.setJvmGCTime(gcTime - startGCTime)
          }
          val reason = new ExceptionFailure(t, metrics)
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }
        }
      } finally {
        // Release memory used by this thread for shuffles
        env.shuffleMemoryManager.releaseMemoryForThisThread()
        // Release memory used by this thread for unrolling blocks
        env.blockManager.memoryStore.releaseUnrollMemoryForThisThread()
        // Release memory used by this thread for accumulators
        Accumulators.clear()
        runningTasks.remove(taskId)
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Class.forName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[String],
          classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    // 获取hadoop配置文件
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    
    // 这里，是不是用java的synchronized进行了多线程并发访问的同步
    // 为什么要这样子做？
    // 因为task实际上是以java线程的方式，在一个CoarseGrainedExecutorBackend进程内并发运行的
    // 如果在执行业务逻辑的时候，要访问一些共享的资源
    // 那么久可能会吹西安多线程并发访问安全问题
    // 所以，在这里spark选择进行了多线程并发访问的同步(synchronized)
    // 为什么在这里要进行多线程并发访问的同步？因为是不是在里面访问了诸如，currentFiles等等，这类共享资源
    synchronized {
      // Fetch missing dependencies
      // 遍历要拉取的文件
      // 通过Utils的fetchFile()方法，通过网络通信，从远程拉取文件
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      // 遍历要拉取的jar
      for ((name, timestamp) <- newJars) {
        // 判断了一下时间戳，要求jar当前时间戳必须小于目标时间戳
        // 通过Utils的fetchFile()方法，拉取jar文件
        val localName = name.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory, localName).toURI.toURL
          if (!urlClassLoader.getURLs.contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  def startDriverHeartbeater() {
    val interval = conf.getInt("spark.executor.heartbeatInterval", 10000)
    val timeout = AkkaUtils.lookupTimeout(conf)
    val retryAttempts = AkkaUtils.numRetries(conf)
    val retryIntervalMs = AkkaUtils.retryWaitMs(conf)
    val heartbeatReceiverRef = AkkaUtils.makeDriverRef("HeartbeatReceiver", conf, env.actorSystem)

    val t = new Thread() {
      override def run() {
        // Sleep a random interval so the heartbeats don't end up in sync
        Thread.sleep(interval + (math.random * interval).asInstanceOf[Int])

        while (!isStopped) {
          val tasksMetrics = new ArrayBuffer[(Long, TaskMetrics)]()
          val curGCTime = gcTime

          for (taskRunner <- runningTasks.values()) {
            if (taskRunner.attemptedTask.nonEmpty) {
              Option(taskRunner.task).flatMap(_.metrics).foreach { metrics =>
                metrics.updateShuffleReadMetrics()
                metrics.updateInputMetrics()
                metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)

                if (isLocal) {
                  // JobProgressListener will hold an reference of it during
                  // onExecutorMetricsUpdate(), then JobProgressListener can not see
                  // the changes of metrics any more, so make a deep copy of it
                  val copiedMetrics = Utils.deserialize[TaskMetrics](Utils.serialize(metrics))
                  tasksMetrics += ((taskRunner.taskId, copiedMetrics))
                } else {
                  // It will be copied by serialization
                  tasksMetrics += ((taskRunner.taskId, metrics))
                }
              }
            }
          }

          val message = Heartbeat(executorId, tasksMetrics.toArray, env.blockManager.blockManagerId)
          try {
            val response = AkkaUtils.askWithReply[HeartbeatResponse](message, heartbeatReceiverRef,
              retryAttempts, retryIntervalMs, timeout)
            if (response.reregisterBlockManager) {
              logWarning("Told to re-register on heartbeat")
              env.blockManager.reregister()
            }
          } catch {
            case NonFatal(t) => logWarning("Issue communicating with driver in heartbeater", t)
          }

          Thread.sleep(interval)
        }
      }
    }
    t.setDaemon(true)
    t.setName("Driver Heartbeater")
    t.start()
  }
}
