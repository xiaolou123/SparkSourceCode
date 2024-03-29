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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{TimerTask, Timer}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.language.postfixOps
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.util.Utils
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId


/**
 * 1、底层通过操作一个SchedulerBackend，针对不同种类的cluseter（standalone、yarn、mesos），调度task
 * 2、它也可以通过使用一个LocalBackend，并且将isLocal参数设置为true，来在本地模式下工作
 * 3、它负责处理一些通用的逻辑，比如说决定多个job的调度顺序，启动推测任务执行
 * 4、客户端首先应用调用它的initialize()方法和start()方法，然后通过runTasks()方法提交task sets
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging
{
  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = conf.getLong("spark.speculation.interval", 100)

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT = conf.getLong("spark.starvation.timeout", 15000)

  // CPUs to request per task
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  val activeTaskSets = new HashMap[String, TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  protected val executorsByHost = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()


  override def start() {
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      import sc.env.actorSystem.dispatcher
      sc.env.actorSystem.scheduler.schedule(SPECULATION_INTERVAL milliseconds,
            SPECULATION_INTERVAL milliseconds) {
        Utils.tryOrExit { checkSpeculatableTasks() }
      }
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }



  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    activeTaskSets.find(_._2.stageId == stageId).foreach { case (_, tsm) =>
      // There are two possible cases here:
      // 1. The task set manager has been created and some tasks have been scheduled.
      //    In this case, send a kill signal to the executors to kill the task and then abort
      //    the stage.
      // 2. The task set manager has been created but no tasks has been scheduled. In this case,
      //    simply abort the stage.
      tsm.runningTasksSet.foreach { tid =>
        val execId = taskIdToExecutorId(tid)
        backend.killTask(tid, execId, interruptThread)
      }
      tsm.abort("Stage %s cancelled".format(stageId))
      logInfo("Stage %d was cancelled".format(stageId))
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    activeTaskSets -= manager.taskSet.id
    manager.parent.removeSchedulable(manager)
    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
      .format(manager.taskSet.id, manager.parent.name))
  }

  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
   
    // 遍历所有executor
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      
      // 如果当前executor的cpu数量至少大于每个task要使用的cpu数量，默认是1
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          
          // 调用TaskSetManager的resourceOffer方法
          // 去找到，在这个executor上，就用这种本地化级别，taskeset哪些task可以启动
          // 遍历使用当前本地化级别，可以在该executor上启动的task
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            // 放入taskes这个二位数组吧，给指定的executor加上要启动的task
            tasks(i) += task
            // 到这里为止，其实，就是task分配算法的实现了
            // 我们尝试着用本地化级别这种模型，去优化task的分配和启动，优先希望在最佳本地化的地方启动task
            // 然后呢，将task分配给executor
            
            // 将相应的分配信息加入内存缓存
            val tid = task.taskId
            taskIdToTaskSetId(tid) = taskSet.taskSet.id
            taskIdToExecutorId(tid) = execId
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            
            //标识为true
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      activeExecutorIds += o.executorId
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    // 首先，将可用的executor进行shuffle，也就是说，进行打散，从而做到，尽量可以进行负载均衡
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    
    // 然后针对WorkerOffer，创建出一堆需要用的东西
    // 比如tasks，很重要，它呢可以理解为一个二维数组，ArrayBuffer，元素又是一个ArrayBuffer
    // 并且每个子ArrayBuffer的数量是固定的，也就是这个executor可用的cpu数量
    val tasks = shuffledOffers .map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    
    // 这个很重要，从rootPool中取出排序的TaskSet
    // 之前讲解TaskScheduler初始化的时候，我们知道，创建完TaskSchedulerImpl、SparkDeploySchedulerBackend之后
    // 执行一个innitialize()方法，再这个方法中，其实会创建一个调度池
    // 这里，相当于是说，所有提交的TaskSet，首先呢，会放入调度池
    // 然后在执行task分配算法的时候，会从这个调度池中，取出拍好队的TaskSet
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    
    // 这里呢，就是任务分配算法的核心了
    // 双重for循环，遍历所有的taskset，以及每一种本地化级别
    // NODE_LOCEL，也就是说，rdd的partition和task，不在一个executor中，不在一个进程，但是在一个worker节点上
    // NO_PREF,无，没有所谓的本地化级别
    // RACK_LOCAL，机架本地化，至少rdd的partition和task，在一个机架上
    // ANY,任意的本地化级别
    
    // 对每个taskset，从最好的一种本地化级别，开始遍历
    
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
        // 对当前taskset
        // 尝试优先使用最小的本地化级别，将taskset的task，在executor上进行启动
        // 如果启动不了，那么久跳初这个do while循环，进入下一种本地化级别，也就是放大本地化级别
        // 以此类推，知道尝试将taskset在某些本地化级别下，让task在executor上全部启动
        launchedTask = resourceOfferSingleTaskSet(
            taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    synchronized {
      try {
        // 判断如果task是lost了，大家在实际编写spark应用程序的时候，可能会经常发现task lost了
        // 这个时候，就是因为各种各样的原因，执行失败了
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          // 这里就会移除executor，将它加入失败队列
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          // 获取对应的taskSet
          case Some(taskSetId) =>
            // 如果task结束了，从内存缓存中移除
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              taskIdToExecutorId.remove(tid)
            }
            
            // 如果正常结束，那么也做相应的处理
            activeTaskSets.get(taskSetId).foreach { taskSet =>
              if (state == TaskState.FINISHED) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
               "likely the result of receiving duplicate task finished status updates)")
              .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
      blockManagerId: BlockManagerId): Boolean = {

    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
      taskMetrics.flatMap { case (id, metrics) =>
        taskIdToTaskSetId.get(id)
          .flatMap(activeTaskSets.get)
          .map(taskSetMgr => (id, taskSetMgr.stageId, taskSetMgr.taskSet.attempt, metrics))
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, metricsWithStageIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskResult: DirectTaskResult[_]) = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
    taskSetManager: TaskSetManager,
    tid: Long,
    taskState: TaskState,
    reason: TaskEndReason) = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (activeTaskSets.size > 0) {
        // Have each task set throw a SparkException with the error
        for ((taskSetId, manager) <- activeTaskSets) {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        logError("Exiting due to error from cluster scheduler: " + message)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism() = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  def executorLost(executorId: String, reason: ExecutorLossReason) {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {
         // We may get multiple executorLost() calls with different loss reasons. For example, one
         // may be triggered by a dropped connection from the slave while another may be a report
         // of executor termination from Mesos. We produce log messages for both so we eventually
         // report the termination reason.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }
    executorIdToHost -= executorId
    rootPool.executorLost(executorId, host)
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

}
