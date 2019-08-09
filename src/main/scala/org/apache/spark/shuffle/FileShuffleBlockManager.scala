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

package org.apache.spark.shuffle

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FileShuffleBlockManager.ShuffleFileGroup
import org.apache.spark.storage._
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}

/** A group of writers for a ShuffleMapTask, one writer per reducer. */
private[spark] trait ShuffleWriterGroup {
  val writers: Array[BlockObjectWriter]

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer (this set of files is called a ShuffleFileGroup).
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle
 * files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFileGroup maintains a list of offsets for
 * each block stored in each file. In order to find the location of a shuffle block, we search the
 * files within a ShuffleFileGroups associated with the block's reducer.
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.StandaloneShuffleBlockManager#getHashBasedShuffleBlockData().
private[spark]
class FileShuffleBlockManager(conf: SparkConf)
  extends ShuffleBlockManager with Logging {

  private val transportConf = SparkTransportConf.fromSparkConf(conf)

  private lazy val blockManager = SparkEnv.get.blockManager

  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  private val consolidateShuffleFiles =
    conf.getBoolean("spark.shuffle.consolidateFiles", false)

  private val bufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  /**
   * Contains all the state related to a particular shuffle. This includes a pool of unused
   * ShuffleFileGroups, as well as all ShuffleFileGroups that have been created for the shuffle.
   */
  private class ShuffleState(val numBuckets: Int) {
    val nextFileId = new AtomicInteger(0)
    val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()
    val allFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()

    /**
     * The mapIds of all map tasks completed on this Executor for this shuffle.
     * NB: This is only populated if consolidateShuffleFiles is FALSE. We don't need it otherwise.
     */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * Get a ShuffleWriterGroup for the given map task, which will register it as complete
   * when the writers are closed successfully
   */
  /**
   * 给每个map task获取一个ShuffleWriterGroup
   */
  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics) = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
      private val shuffleState = shuffleStates(shuffleId)
      private var fileGroup: ShuffleFileGroup = null

      // 这里就很关键了
      // 对应上我们之前所说的，shuffle有两种模式吧，一种是普通的，一种是优化后的
      // 那么这里会判断，如果开启了consolidation机制，也即使consolidateShuffleFiles为true的话
      // 那么实际上，不会给每个bucket都获取一个独立的文件
      // 而是为这个bucket，获取一个ShuffleGroup的writer
      val writers: Array[BlockObjectWriter] = if (consolidateShuffleFiles) {
        fileGroup = getUnusedFileGroup()
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          // 首先，用shuffleId、mapId、bucketId（reduceId）生成一个唯一的ShuffleBlockId
          // 然后用bucketId，来调用ShuffleFileGroup的apply()函数，为bucket获取一个ShuffleFileGroup
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          // 然后用BlockManger的getDiskWriter()方法，针对ShuffleFileGroup获取一个Writer
          // 这样的话，我们就清楚了，如果开启了consolidation机制
          // 实际上，对于每一个bucket，都会获取一个正对SheffleFileGroup的writer，而不是一个独立的ShuffleBlockFile的writer
          // 这样就实想了所谓的，多个ShuffleMapTask的输出数据的合并
          blockManager.getDiskWriter(blockId, fileGroup(bucketId), serializer, bufferSize,
            writeMetrics)
        }
      } else {
        // 如果没有开启consolidation机制，也就是普通shuffle操作的话
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          // 同样生成一个ShuffleBlockId
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          // 然后调用BlockManager的DiskBlockManager，获取一个代表了要写入的本地磁盘文件的blockFile
          val blockFile = blockManager.diskBlockManager.getFile(blockId)
          // Because of previous failures, the shuffle file may already exist on this machine.
          // If so, remove it.
          // 而且会判断，这个blockFile要是存在的话，还得删除它
          if (blockFile.exists) {
            if (blockFile.delete()) {
              logInfo(s"Removed existing shuffle file $blockFile")
            } else {
              logWarning(s"Failed to remove existing shuffle file $blockFile")
            }
          }
          // 然后调用blockManager的getDiskWriter()方法，针对哪个blockFiles生成writer
          blockManager.getDiskWriter(blockId, blockFile, serializer, bufferSize, writeMetrics)
        
          // 所以使用这种普通的shuffle操作的话
          // 对于每一个ShuffleMapTask输出的bucket，那么都会在本地获取一个单独的shuffleBlockFile
        }
      }

      override def releaseWriters(success: Boolean) {
        if (consolidateShuffleFiles) {
          if (success) {
            val offsets = writers.map(_.fileSegment().offset)
            val lengths = writers.map(_.fileSegment().length)
            fileGroup.recordMapOutput(mapId, offsets, lengths)
          }
          recycleFileGroup(fileGroup)
        } else {
          shuffleState.completedMapTasks.add(mapId)
        }
      }

      private def getUnusedFileGroup(): ShuffleFileGroup = {
        val fileGroup = shuffleState.unusedFileGroups.poll()
        if (fileGroup != null) fileGroup else newFileGroup()
      }

      private def newFileGroup(): ShuffleFileGroup = {
        val fileId = shuffleState.nextFileId.getAndIncrement()
        val files = Array.tabulate[File](numBuckets) { bucketId =>
          val filename = physicalFileName(shuffleId, bucketId, fileId)
          blockManager.diskBlockManager.getFile(filename)
        }
        val fileGroup = new ShuffleFileGroup(shuffleId, fileId, files)
        shuffleState.allFileGroups.add(fileGroup)
        fileGroup
      }

      private def recycleFileGroup(group: ShuffleFileGroup) {
        shuffleState.unusedFileGroups.add(group)
      }
    }
  }

  override def getBytes(blockId: ShuffleBlockId): Option[ByteBuffer] = {
    val segment = getBlockData(blockId)
    Some(segment.nioByteBuffer())
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    if (consolidateShuffleFiles) {
      // Search all file groups associated with this shuffle.
      val shuffleState = shuffleStates(blockId.shuffleId)
      val iter = shuffleState.allFileGroups.iterator
      while (iter.hasNext) {
        val segmentOpt = iter.next.getFileSegmentFor(blockId.mapId, blockId.reduceId)
        if (segmentOpt.isDefined) {
          val segment = segmentOpt.get
          return new FileSegmentManagedBuffer(
            transportConf, segment.file, segment.offset, segment.length)
        }
      }
      throw new IllegalStateException("Failed to find shuffle block: " + blockId)
    } else {
      val file = blockManager.diskBlockManager.getFile(blockId)
      new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
    }
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle. */
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        if (consolidateShuffleFiles) {
          for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {
            file.delete()
          }
        } else {
          for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
            val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
            blockManager.diskBlockManager.getFile(blockId).delete()
          }
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  override def stop() {
    metadataCleaner.cancel()
  }
}

private[spark]
object FileShuffleBlockManager {
  /**
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[File]) {
    private var numBlocks: Int = 0

    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
     */
    private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets and lengths of blocks into each reducer file, ordered by
     * position in the file.
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }
    private val blockLengthsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    def apply(bucketId: Int) = files(bucketId)

    def recordMapOutput(mapId: Int, offsets: Array[Long], lengths: Array[Long]) {
      assert(offsets.length == lengths.length)
      mapIdToIndex(mapId) = numBlocks
      numBlocks += 1
      for (i <- 0 until offsets.length) {
        blockOffsetsByReducer(i) += offsets(i)
        blockLengthsByReducer(i) += lengths(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists. */
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId)
      val blockOffsets = blockOffsetsByReducer(reducerId)
      val blockLengths = blockLengthsByReducer(reducerId)
      val index = mapIdToIndex.getOrElse(mapId, -1)
      if (index >= 0) {
        val offset = blockOffsets(index)
        val length = blockLengths(index)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
