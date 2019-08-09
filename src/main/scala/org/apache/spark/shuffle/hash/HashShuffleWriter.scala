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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.BlockObjectWriter

private[spark] class HashShuffleWriter[K, V](
    shuffleBlockManager: FileShuffleBlockManager,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions
  private val metrics = context.taskMetrics

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null))
  private val shuffle = shuffleBlockManager.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,
    writeMetrics)

  /** Write a bunch of records to this task's output */
  /**
   * 将每个ShuffleMapTask计算出来的新的RDD的partition数据，写入本地磁盘
   */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    // 首先判断，是否需要在map端本地进行聚合
    // 这里的话，如果reduceByKey这种操作，它的dep.aggregator.isDegined就是true
    // 包括def.mapSideCombine也是true
    // 那么就会进行map端的本地聚合
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // 这里就会执行本地聚合
        // 比如本地这里有(hello,1),(hello,1)
        // 那么此时，就会聚合成(hello,2)
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    // 如果要本地聚合，那么先本地聚合
    // 然后遍历数据
    // 对每个数据，调用partitionner，默认是HashPartitioner，生成bucketId
    // 也就是决定了，每一份数据，要写入哪个bucket中
    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      // 会调用shuffleBlockManager.forMapTask()方法，来生成bucketId对应的Writer，然后用Writer将数据写入bucket
      shuffle.writers(bucketId).write(elem)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        try {
          Some(commitWritesAndBuildStatus())
        } catch {
          case e: Exception =>
            success = false
            revertWrites()
            throw e
        }
      } else {
        revertWrites()
        None
      }
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
    }
  }

  private def commitWritesAndBuildStatus(): MapStatus = {
    // Commit the writes. Get the size of each bucket block (total block size).
    val sizes: Array[Long] = shuffle.writers.map { writer: BlockObjectWriter =>
      writer.commitAndClose()
      writer.fileSegment().length
    }
    MapStatus(blockManager.shuffleServerId, sizes)
  }

  private def revertWrites(): Unit = {
    if (shuffle != null && shuffle.writers != null) {
      for (writer <- shuffle.writers) {
        writer.revertPartialWritesAndClose()
      }
    }
  }
}
