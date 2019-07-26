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

package org.apache.spark.deploy.worker

import java.io._

import scala.collection.JavaConversions._

import akka.actor.ActorRef
import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.util.{Clock, SystemClock}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 */
private[spark] class DriverRunner(
    val conf: SparkConf,
    val driverId: String,
    val workDir: File,
    val sparkHome: File,
    val driverDesc: DriverDescription,
    val worker: ActorRef,
    val workerUrl: String)
  extends Logging {

  @volatile var process: Option[Process] = None
  @volatile var killed = false

  // Populated once finished
  var finalState: Option[DriverState] = None
  var finalException: Option[Exception] = None
  var finalExitCode: Option[Int] = None

  // Decoupled for testing
  private[deploy] def setClock(_clock: Clock) = clock = _clock
  private[deploy] def setSleeper(_sleeper: Sleeper) = sleeper = _sleeper
  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile(f => {Thread.sleep(1000); !killed})
  }

  /** Starts a thread to run and manage the driver. */
  def start() = {
    //DriverRunner机制剖析
    
    //启动一个java的线程
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        try {
          //第一步，创建driver的工作目录
          val driverDir = createWorkingDirectory()
          //第二步，下载用户上传的jar（我们编写完了spark应用程序，如果是java，用maven打个jar包，）
          //如果是scala，那么会用export将它导出为jar包
          val localJarFilename = downloadUserJar(driverDir)

          def substituteVariables(argument: String): String = argument match {
            case "{{WORKER_URL}}" => workerUrl
            case "{{USER_JAR}}" => localJarFilename
            case other => other
          }

          // TODO: If we add ability to submit multiple jars they should also be added here
          // 构建了ProcessBuilder
          // 传入了driver的启动命令，需要的内存大小等信息
          val builder = CommandUtils.buildProcessBuilder(driverDesc.command, driverDesc.mem,
            sparkHome.getAbsolutePath, substituteVariables)
          // 通过ProcessBuilder启动Driver进程
          launchDriver(builder, driverDir, driverDesc.supervise)
        }
        catch {
          case e: Exception => finalException = Some(e)
        }

        //对driver的退出状态做一些处理
        val state =
          if (killed) {
            DriverState.KILLED
          } else if (finalException.isDefined) {
            DriverState.ERROR
          } else {
            finalExitCode match {
              case Some(0) => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)

        // 这个DriverRunner线程，向它所属的worker的actor，发送一个DriverStateChanged的事件
        worker ! DriverStateChanged(driverId, state, finalException)
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  def kill() {
    synchronized {
      process.foreach(p => p.destroy())
      killed = true
    }
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  /**
   * 将用户jar包下载到提供的目录中（之前创建的driver工作目录吧），并返回它在worker本地的路径
   * 如果下载jar包的过程中出现了任何异常，那么会抛出
   */
  private def downloadUserJar(driverDir: File): String = {
    //用hadoop jar里的path
    val jarPath = new Path(driverDesc.jarUrl)
    //拿到了hadoop配置
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    //获取了HDFS的FileSystem
    val jarFileSystem = jarPath.getFileSystem(hadoopConf)
    //创建本地目录
    val destPath = new File(driverDir.getAbsolutePath, jarPath.getName)
    val jarFileName = jarPath.getName
    val localJarFile = new File(driverDir, jarFileName)
    val localJarFilename = localJarFile.getAbsolutePath
    //如果jar在本地不存在
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar $jarPath to $destPath")
      //用FileUtil将jar拷贝到本地
      FileUtil.copy(jarFileSystem, jarPath, destPath, false, hadoopConf)
    }
    //如果拷贝完了，发现jar还是不在，那么就抛出异常
    if (!localJarFile.exists()) { // Verify copy succeeded
      throw new Exception(s"Did not see expected jar $jarFileName in $driverDir")
    }

    localJarFilename
  }

  private def launchDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean) {
    builder.directory(baseDir)
    def initialize(process: Process) = {
      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val header = "Launch Command: %s\n%s\n\n".format(
        builder.command.mkString("\"", "\" \"", "\""), "=" * 40)
      Files.append(header, stderr, UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  private[deploy] def runCommandWithRetry(command: ProcessBuilderLike, initialize: Process => Unit,
    supervise: Boolean) {
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5

    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) { return }
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      val exitCode = process.get.waitFor()
      if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
        waitSeconds = 1
      }

      if (supervise && exitCode != 0 && !killed) {
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }

      keepTrying = supervise && exitCode != 0 && !killed
      finalExitCode = Some(exitCode)
    }
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int)
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder) = new ProcessBuilderLike {
    def start() = processBuilder.start()
    def command = processBuilder.command()
  }
}
