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

package org.apache.spark.storage

import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, Utils}

/**
 * BlockManagerMasterActor is an actor on the master node to track statuses of
 * all slaves' block managers.
 */
/**
 * BlockManagerMasterActor，就是负责维护各个executor的BlockManager的元数据
 * BlockManagerInfo，BlockStatus
 */
private[spark]
class BlockManagerMasterActor(val isLocal: Boolean, conf: SparkConf, listenerBus: LiveListenerBus)
  extends Actor with ActorLogReceive with Logging {

  // Mapping from block manager id to the block manager's information.
  // 这个map，映射了block manager id 到block manager的info
  // BlockManagerId-BlockManagerInfo的映射
  // BlockManagerMaster要负责维护每个BlockManager的BlockManagerInfo
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  // 还有一个映射，就是说，映射了每个executorId到BlockManagerId
  // 也就是说，每个executor是与一个BlockManager相关联的
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val akkaTimeout = AkkaUtils.askTimeout(conf)

  val slaveTimeout = conf.getLong("spark.storage.blockManagerSlaveTimeoutMs", 120 * 1000)

  val checkTimeoutInterval = conf.getLong("spark.storage.blockManagerTimeoutIntervalMs", 60000)

  var timeoutCheckingTask: Cancellable = null

  override def preStart() {
    import context.dispatcher
    timeoutCheckingTask = context.system.scheduler.schedule(0.seconds,
      checkTimeoutInterval.milliseconds, self, ExpireDeadHosts)
    super.preStart()
  }

  override def receiveWithLogging = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveActor) =>
      register(blockManagerId, maxMemSize, slaveActor)
      sender ! true

    case UpdateBlockInfo(
      blockManagerId, blockId, storageLevel, deserializedSize, size, tachyonSize) =>
      sender ! updateBlockInfo(
        blockManagerId, blockId, storageLevel, deserializedSize, size, tachyonSize)

    case GetLocations(blockId) =>
      sender ! getLocations(blockId)

    case GetLocationsMultipleBlockIds(blockIds) =>
      sender ! getLocationsMultipleBlockIds(blockIds)

    case GetPeers(blockManagerId) =>
      sender ! getPeers(blockManagerId)

    case GetActorSystemHostPortForExecutor(executorId) =>
      sender ! getActorSystemHostPortForExecutor(executorId)

    case GetMemoryStatus =>
      sender ! memoryStatus

    case GetStorageStatus =>
      sender ! storageStatus

    case GetBlockStatus(blockId, askSlaves) =>
      sender ! blockStatus(blockId, askSlaves)

    case GetMatchingBlockIds(filter, askSlaves) =>
      sender ! getMatchingBlockIds(filter, askSlaves)

    case RemoveRdd(rddId) =>
      sender ! removeRdd(rddId)

    case RemoveShuffle(shuffleId) =>
      sender ! removeShuffle(shuffleId)

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      sender ! removeBroadcast(broadcastId, removeFromDriver)

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      sender ! true

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      sender ! true

    case StopBlockManagerMaster =>
      sender ! true
      if (timeoutCheckingTask != null) {
        timeoutCheckingTask.cancel()
      }
      context.stop(self)

    case ExpireDeadHosts =>
      expireDeadHosts()

    case BlockManagerHeartbeat(blockManagerId) =>
      sender ! heartbeatReceived(blockManagerId)

    case other =>
      logWarning("Got unknown message: " + other)
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    import context.dispatcher
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveActor.ask(removeMsg)(akkaTimeout).mapTo[Int]
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterActor data structures
    import context.dispatcher
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveActor.ask(removeMsg)(akkaTimeout).mapTo[Boolean]
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    import context.dispatcher
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveActor.ask(removeMsg)(akkaTimeout).mapTo[Int]
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    // 尝试根据blockManagerId获取到它对应的BlockManagerInfo
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    // 从blockManagerIdByExecutor map中移除executorId对应的BlockManagerInfo
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    // 从blockManagerInfo map中也移除blockManagerInfo
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    // 遍历BlockManagerInfo内部所有的block块对应的BlockStatus块的blockId
    while (iterator.hasNext) {
      // 清空BlockManagerInfo内部所有的block的BlockStatus信息
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.size == 0) {
        blockLocations.remove(blockId)
      }
    }
    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def expireDeadHosts() {
    logTrace("Checking for hosts with no recent heart beats in BlockManagerMaster.")
    val now = System.currentTimeMillis()
    val minSeenTime = now - slaveTimeout
    val toRemove = new mutable.HashSet[BlockManagerId]
    for (info <- blockManagerInfo.values) {
      if (info.lastSeenMs < minSeenTime && !info.blockManagerId.isDriver) {
        logWarning("Removing BlockManager " + info.blockManagerId + " with no recent heart beats: "
          + (now - info.lastSeenMs) + "ms exceeds " + slaveTimeout + "ms")
        toRemove += info.blockManagerId
      }
    }
    toRemove.foreach(removeBlockManager)
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    // 获取executorId对应的BlockManagerInfo，对其调用removeBlockManager()方法
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveActor.ask(RemoveBlock(blockId))(akkaTimeout)
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    import context.dispatcher
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master actor should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master actor's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveActor.ask(getBlockStatus)(akkaTimeout).mapTo[Option[BlockStatus]]
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    import context.dispatcher
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveActor.ask(getMatchingBlockIds)(akkaTimeout).mapTo[Seq[BlockId]]
          } else {
            Future { info.blocks.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  /**
   *  注册BlockManager
   */
  private def register(id: BlockManagerId, maxMemSize: Long, slaveActor: ActorRef) {
    val time = System.currentTimeMillis()
    // 首先判断，如果本地HashMap中没有指定的BlockManagerId，说明从来没有注册过
    // 那么才会往下走，去注册这个BlockManager
    if (!blockManagerInfo.contains(id)) {
      
      // 根据blockManager对应的executorId找到对应的BlockManagerInfo
      // 这里其实是做一个安全判断
      // 因为，如果blockManagerInfo map里，没有BlockManagerId
      // 那么同步的blockManagerIdByExecutor map里，也必须没有
      // 所以这里要判断一下，如果blockManagerIdByExecutor map里有BlockManagerId，那么做一下清理
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - " 
              + s" will replace old one $oldId with new one $id")
          // 从内存中，移除掉executorId相关的blockManagerInfo
          removeExecutor(id.executorId)  
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))
      
      // 往blockManagerIdByExecutor map中保存一份executorId到blockManagerId的映射
      blockManagerIdByExecutor(id.executorId) = id
      
      // 为blockManagerId创建一份BlockManagerInfo
      // 并往blockManagerInfo map中，保存一份blockManagerId到BlockManagerInfo的映射
      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveActor)
    }
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxMemSize))
  }

  /**
   * 更新blockInfo
   * 也就是说，每个BlockManager上，如果block发生了变化，那么都要发送updateBlockInfo请求来BlockManagerMaster这里，
   * 进行BlockInfo的更新
   */
  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      tachyonSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    // 调用blockManager的BlockManagerInfo的updateBlockInfo()方法， 更新block信息
    blockManagerInfo(blockManagerId).updateBlockInfo(
      blockId, storageLevel, memSize, diskSize, tachyonSize)

    // 每一个block可能会在多个BlockManager上面
    // 因为如果讲StorageLevel设置城带着_2的这种，那么久需要将block replicate一份，放到其他BlockManager上
    // blockLocations map，其实保存了每个blockId对应的BlockManagerId的set集合
    // 所以这里，会更新blockLocations中的信息，因为是用set存储BlockManagerId，因此自动就去重了  
    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(blockIds: Array[BlockId]): Seq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns the hostname and port of an executor's actor system, based on the Akka address of its
   * BlockManagerSlaveActor.
   */
  private def getActorSystemHostPortForExecutor(executorId: String): Option[(String, Int)] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId);
      host <- info.slaveActor.path.address.host;
      port <- info.slaveActor.path.address.port
    ) yield {
      (host, port)
    }
  }
}

@DeveloperApi
case class BlockStatus(
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long,
    tachyonSize: Long) {
  def isCached: Boolean = memSize + diskSize + tachyonSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, 0L, 0L, 0L)
}

/**
 * 每一个BlockManager的BlockManagerInfo
 * 相当于是BlockManager的元数据
 */
private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxMem: Long,
    val slaveActor: ActorRef)
  extends Logging {

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  // BlockManagerInfo管理了每个BlockManager内部的block的blockId-BlockStatus的映射
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  def getStatus(blockId: BlockId) = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      tachyonSize: Long) {

    updateLastSeenMs()

    /**
     * 判断，如果内部，有这个block
     */
    if (_blocks.containsKey(blockId)) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize

      // 判断如果storageLevel是使用内存，那么久给剩余内存数量加上当前的内存量
      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    // 给block创建一份BlockStatus，然后根据其持久化级别，对响应的内存资源进行计算
    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory, on-disk or on-Tachyon.
       * The memSize here indicates the data size in or dropped from memory,
       * tachyonSize here indicates the data size in or dropped from Tachyon,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      if (storageLevel.useMemory) {
        _blocks.put(blockId, BlockStatus(storageLevel, memSize, 0, 0))
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        _blocks.put(blockId, BlockStatus(storageLevel, 0, diskSize, 0))
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (storageLevel.useOffHeap) {
        _blocks.put(blockId, BlockStatus(storageLevel, 0, 0, tachyonSize))
        logInfo("Added %s on tachyon on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(tachyonSize)))
      }
    // 如果StorageLevel是非法的，而且之前保存过这个blockId
    // 那么久将blockId从内存中删除
    } else if (_blocks.containsKey(blockId)) {
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
      if (blockStatus.storageLevel.useOffHeap) {
        logInfo("Removed %s on %s on tachyon (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.tachyonSize)))
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
