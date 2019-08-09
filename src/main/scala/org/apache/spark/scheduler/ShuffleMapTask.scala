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

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */

/**
 * 一个ShuffleMapTask会将一个RDD的元素，切分为多个bucket
 * 基于一个在SheffleDependency中指定的partitioner，默认呢，就是HashPartitioner
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  /**
   * 非常重要的一点，就是ShuffleMapTask的runTask()方法，有MapStatus返回值
   */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    // 对task要处理的rdd相关的数据，做一些反序列化操作
    // 这个rdd，关键问题是，你是怎么拿到的？？
    // 因为大家知道，多个task运行在多个executor中，都是并行运行，或者并发运行的
    // 可能都不在一个地方
    // 但是呢？一个stage的task，其实要处理的rdd是一样的
    // 所以task怎么拿到自己要处理的哪个rdd的数据呢？
    // 这里呢，会通过broadcast variable，直接拿到
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获取SheffleManager
      // 从ShuffleManager中获取ShuffleWriter
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 最最重要的一行代码就在这里
      // 首先调用了，rdd的iterator()方法，并且传入了，当前task要处理哪个partition
      // 所以，核心的逻辑，就在rdd的iterator()方法中，在这里，就实现了针对rdd的某个partition，执行我们自定义的
      // 算子，或者函数
      // 执行完了我们自己定义的算子，或者函数，是不是相当于是，很对rdd的partition执行了处理，那么是不是会有返回
      // 的数据？？
      // ok，返回的数据，都是通过ShuffleWriter，经过HashPartitioner进行分区之后，写入自己对应的分区bucket 
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // 最后，返回结果，MapStatus
      // MapStatus里面封装了ShuffleMapTask计算后的数据，存储在哪里，其实就是BlockManager相关的信息
      // BlockManager，是Spark底层的内存、数据、磁盘数据管理的组件
      // 讲完Shuffle之后，我们就来剖析BlockManager
      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
