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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockFetcherIterator, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

private[hash] object BlockStoreShuffleFetcher extends Logging {
  def fetch[T](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer)
    : Iterator[T] =
  {
    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    
    // 这一步非常非常关键，一定要理解
    // 拿到了一个全局的MapOutputTrackerMaster的引用
    // 然后调用其方法getServerStatuses()方法，这里传入的参数一定要注意，注意，注意！！
    // 传入了shuffleId和reduceId
    // shuffle write发生在上一个stage中，shuffle read是发生在当前的stage中的
    // 这么来理解
    // 首先通过shuffleId可以闲置到上一个stage的所有ShuffleMapTask的输出MapStatus
    // 接着，通过reduceId，也就是所谓的bucketId，来闲置，从每个MapStatus中，获取当前这个ResultTask需要
    // 获取的每个ShuffleMapTask的输出文件的信息
    // 这里要注意，这个getServerStatused()方法，一定是走远程网络通信的，
    // 因为要联系Driver上的DAGScheduler的MapOutputTrackerMaster
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    // 这一打开快代码，是干嘛的
    // 其实就是对刚才拉取到的信息，statues，进行一些数据结构上的转换操作
    // 比如，弄成map格式的数据
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2)))
    }

    def unpackBlock(blockPair: (BlockId, Try[Iterator[Any]])) : Iterator[T] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Success(block) => {
          block.asInstanceOf[Iterator[T]]
        }
        case Failure(e) => {
          blockId match {
            case ShuffleBlockId(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block", e)
          }
        }
      }
    }

    // 这个代码，非常之重要
    // ShuffleBlockFetcherItarator构造以后，在其内部，就直接根据拉取到的地理位置信息，通过BlockManager
    // 去远程的ShuffleMapTask所在节点的blockManager去拉取数据
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      serializer,
      SparkEnv.get.conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)
    val itr = blockFetcherItr.flatMap(unpackBlock)

    // 最后，将拉取到的数据，进行一些转换，和封装
    // 返回
    val completionIter = CompletionIterator[T, Iterator[T]](itr, {
      context.taskMetrics.updateShuffleReadMetrics()
    })

    new InterruptibleIterator[T](context, completionIter) {
      val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
      override def next(): T = {
        readMetrics.incRecordsRead(1)
        delegate.next()
      }
    }
  }
}
