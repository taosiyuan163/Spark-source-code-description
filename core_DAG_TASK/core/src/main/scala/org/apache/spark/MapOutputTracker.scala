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

package org.apache.spark

import java.io._
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util._

/**
 * Helper class used by the [[MapOutputTrackerMaster]] to perform bookkeeping for a single
 * ShuffleMapStage.
 *
 * This class maintains a mapping from mapIds to `MapStatus`. It also maintains a cache of
 * serialized map statuses in order to speed up tasks' requests for map output statuses.
 *
 * All public methods of this class are thread-safe.
 */
// 通过MapOutputTrackerMaster的registerShuffle生成
// 用来维护单个ShuffleMapStage的mapIds 到 MapStatus的映射关系
private class ShuffleStatus(numPartitions: Int) {

  // All accesses to the following state must be guarded with `this.synchronized`.

  /**
   * MapStatus for each partition. The index of the array is the map partition id.
   * Each value in the array is the MapStatus for a partition, or null if the partition
   * is not available. Even though in theory a task may run multiple times (due to speculation,
   * stage retries, etc.), in practice the likelihood of a map output being available at multiple
   * locations is so small that we choose to ignore that case and store only a single location
   * for each output.
   */
  // 索引长度是分区数，里面维护着每个partition对应的MapStatus
  // MapStatus中维护的是BlockManagerId，也就是每个task运行的位置和每个reduce task接收的block大小
  private[this] val mapStatuses = new Array[MapStatus](numPartitions)

  /**
   * The cached result of serializing the map statuses array. This cache is lazily populated when
   * [[serializedMapStatus]] is called. The cache is invalidated when map outputs are removed.
   */
  private[this] var cachedSerializedMapStatus: Array[Byte] = _

  /**
   * Broadcast variable holding serialized map output statuses array. When [[serializedMapStatus]]
   * serializes the map statuses array it may detect that the result is too large to send in a
   * single RPC, in which case it places the serialized array into a broadcast variable and then
   * sends a serialized broadcast variable instead. This variable holds a reference to that
   * broadcast variable in order to keep it from being garbage collected and to allow for it to be
   * explicitly destroyed later on when the ShuffleMapStage is garbage-collected.
   */
  // 如果map output 状态信息太大 无法被单个RPC发送
  // 那就存储到这个cachedSerializedBroadcast序列化的广播变量数组里
  private[this] var cachedSerializedBroadcast: Broadcast[Array[Byte]] = _

  /**
   * Counter tracking the number of partitions that have output. This is a performance optimization
   * to avoid having to count the number of non-null entries in the `mapStatuses` array and should
   * be equivalent to`mapStatuses.count(_ ne null)`.
   */
  private[this] var _numAvailableOutputs: Int = 0

  /**
   * Register a map output. If there is already a registered location for the map output then it
   * will be replaced by the new location.
   */
  def addMapOutput(mapId: Int, status: MapStatus): Unit = synchronized {
    // 如果发现对应的mapId为空
    if (mapStatuses(mapId) == null) {
      //增加一个Output的计数次数
      _numAvailableOutputs += 1
      //清除MapOutputStatus的序列化缓存
      invalidateSerializedMapOutputStatusCache()
    }
    //最后直接替换掉原有的status
    mapStatuses(mapId) = status
  }

  /**
   * Remove the map output which was served by the specified block manager.
   * This is a no-op if there is no registered map output or if the registered output is from a
   * different block manager.
   */
  def removeMapOutput(mapId: Int, bmAddress: BlockManagerId): Unit = synchronized {
    if (mapStatuses(mapId) != null && mapStatuses(mapId).location == bmAddress) {
      _numAvailableOutputs -= 1
      mapStatuses(mapId) = null
      invalidateSerializedMapOutputStatusCache()
    }
  }

  /**
   * Removes all map outputs associated with the specified executor. Note that this will also
   * remove outputs which are served by an external shuffle server (if one exists), as they are
   * still registered with that execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = synchronized {
    for (mapId <- 0 until mapStatuses.length) {
      if (mapStatuses(mapId) != null && mapStatuses(mapId).location.executorId == execId) {
        _numAvailableOutputs -= 1
        mapStatuses(mapId) = null
        invalidateSerializedMapOutputStatusCache()
      }
    }
  }

  /**
   * Number of partitions that have shuffle outputs.
   */
  def numAvailableOutputs: Int = synchronized {
    _numAvailableOutputs
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   */
  def findMissingPartitions(): Seq[Int] = synchronized {
    // 遍历每一个partitionId 看是否在mapStatuses中,若为null则过滤掉
    // 这个mapStatuses会在task计算完成之后把对应的partition信息添加进去
    // 所以若是第一次计算 mapStatuses是为空的
    val missing = (0 until numPartitions).filter(id => mapStatuses(id) == null)
    assert(missing.size == numPartitions - _numAvailableOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
    missing
  }

  /**
   * Serializes the mapStatuses array into an efficient compressed format. See the comments on
   * `MapOutputTracker.serializeMapStatuses()` for more details on the serialization format.
   *
   * This method is designed to be called multiple times and implements caching in order to speed
   * up subsequent requests. If the cache is empty and multiple threads concurrently attempt to
   * serialize the map statuses then serialization will only be performed in a single thread and all
   * other threads will block until the cache is populated.
   */
  def serializedMapStatus(
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int): Array[Byte] = synchronized {
    if (cachedSerializedMapStatus eq null) {
      val serResult = MapOutputTracker.serializeMapStatuses(
          mapStatuses, broadcastManager, isLocal, minBroadcastSize)
      cachedSerializedMapStatus = serResult._1
      cachedSerializedBroadcast = serResult._2
    }
    cachedSerializedMapStatus
  }

  // Used in testing.
  def hasCachedSerializedBroadcast: Boolean = synchronized {
    cachedSerializedBroadcast != null
  }

  /**
   * Helper function which provides thread-safe access to the mapStatuses array.
   * The function should NOT mutate the array.
   */
  def withMapStatuses[T](f: Array[MapStatus] => T): T = synchronized {
    f(mapStatuses)
  }

  /**
   * Clears the cached serialized map output statuses.
   */
  def invalidateSerializedMapOutputStatusCache(): Unit = synchronized {
    if (cachedSerializedBroadcast != null) {
      cachedSerializedBroadcast.destroy()
      cachedSerializedBroadcast = null
    }
    cachedSerializedMapStatus = null
  }
}

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

// 主要存放了shuffleId和回调函数RpcCallContext 2个成员变量
private[spark] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)

/** RpcEndpoint class for MapOutputTrackerMaster */
// 只存在Driver的MapOutputTrackerMasterEndpoint
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      // 从回调函数信息中拿到hostPort
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      // 往mapOutputRequests队列里插入查询到的shuffleId和对应的Endpoint返回的message信息
      // 补充:mapOutputRequests是executor向Driver对MapOutputStatus查询请求的线程安全阻塞队列
      val mapOutputStatuses = tracker.post(new GetMapOutputMessage(shuffleId, context))

      // 收到停止StopMapOutputTracker请求
    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of a stage. This is abstract because the
 * driver and executor have different versions of the MapOutputTracker. In principle the driver-
 * and executor-side classes don't need to share a common base class; the current shared base class
 * is maintained primarily for backwards-compatibility in order to avoid having to update existing
 * test code.
*/
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
  /** Set to the MapOutputTrackerMasterEndpoint living on the driver. */
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * The driver-side counter is incremented every time that a map output is lost. This value is sent
   * to executors as part of tasks, where executors compare the new epoch number to the highest
   * epoch number that they received in the past. If the new epoch number is higher then executors
   * will clear their local caches of map output statuses and will re-fetch (possibly updated)
   * statuses from the driver.
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      // 发送一个消息到TrackerEndpoint并且得到它的回应
      // 当调用ask之类的函数的时候，对应的Endpoint就会调用receiveAndReply来接收消息并返回结果
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. */
  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  // For testing
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1)
  }

  /**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given range of map output partitions (startPartition is included but
   * endPartition is excluded from the range).
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])]

  /**
   * Deletes map output status information for the specified shuffle stage.
   */
  def unregisterShuffle(shuffleId: Int): Unit

  def stop() {}
}

/**
 * Driver-side class that keeps track of the location of the map output of a stage.
 *
 * The DAGScheduler uses this class to (de)register map output statuses and to look up statistics
 * for performing locality-aware reduce task scheduling.
 *
 * ShuffleMapStage uses this class for tracking available / missing outputs in order to determine
 * which tasks need to be run.
 */
private[spark] class MapOutputTrackerMaster(
    conf: SparkConf,
    broadcastManager: BroadcastManager,
    isLocal: Boolean)
  extends MapOutputTracker(conf) {

  // The size at which we use Broadcast to send the map output statuses to the executors
  private val minSizeForBroadcast =
    conf.getSizeAsBytes("spark.shuffle.mapOutput.minSizeForBroadcast", "512k").toInt

  /** Whether to compute locality preferences for reduce tasks */
  private val shuffleLocalityEnabled = conf.getBoolean("spark.shuffle.reduceLocality.enabled", true)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task. Making this larger will focus on fewer locations where most data
  // can be read locally, but may lead to more delay in scheduling if those locations are busy.
  private val REDUCER_PREF_LOCS_FRACTION = 0.2

  // HashMap for storing shuffleStatuses in the driver.
  // Statuses are dropped only by explicit de-registering.
  // 在Driver上存放shuffleId和ShuffleStatus的映射关系
  private val shuffleStatuses = new ConcurrentHashMap[Int, ShuffleStatus]().asScala

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // requests for map output statuses
  // 各个节点向Driver端查询map output状态的请求消息都放在这个GetMapOutputMessage类型的线程安全的先进先出阻塞队列里面
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]

  // Thread pool used for handling map output status requests. This is a separate thread pool
  // to ensure we don't block the normal dispatcher threads.
  // 单独的一个线程池用来处理map output状态请求
  private val threadpool: ThreadPoolExecutor = {
    // 默认是8个并行度
    val numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8)
    // 构建java.util.concurrent.ThreadPoolExecutor
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      // 线程池调用java.lang.Runnable
      pool.execute(new MessageLoop)
    }
    pool
  }

  // Make sure that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"spark.shuffle.mapOutput.minSizeForBroadcast ($minSizeForBroadcast bytes) must " +
      s"be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an rpc " +
      "message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  def post(message: GetMapOutputMessage): Unit = {
    //LinkedBlockingQueue队列，offer是往队尾插入元素，如果满了的话就返回false
    mapOutputRequests.offer(message)
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            // 从维护请求消息的map中拿取数据（先进先出）
            val data = mapOutputRequests.take()
             if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
               // 如果是PoisonPill就放回去
              mapOutputRequests.offer(PoisonPill)
              return
            }
            val context = data.context
            val shuffleId = data.shuffleId
            val hostPort = context.senderAddress.hostPort
            logDebug("Handling request to send map output locations for shuffle " + shuffleId +
              " to " + hostPort)
            val shuffleStatus = shuffleStatuses.get(shuffleId).head
            context.reply(
              shuffleStatus.serializedMapStatus(broadcastManager, isLocal, minSizeForBroadcast))
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new GetMapOutputMessage(-99, null)

  // Used only in unit tests.
  private[spark] def getNumCachedSerializedBroadcast: Int = {
    shuffleStatuses.valuesIterator.count(_.hasCachedSerializedBroadcast)
  }

  def registerShuffle(shuffleId: Int, numMaps: Int) {
    // 这里只是生产ShuffleStatus和其中的mapStatuses，还没开始为其加入任何分区元数据
    if (shuffleStatuses.put(shuffleId, new ShuffleStatus(numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    shuffleStatuses(shuffleId).addMapOutput(mapId, status)
  }

  /** Unregister map output information of the given shuffle, mapper and block manager */
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.removeMapOutput(mapId, bmAddress)
        incrementEpoch()
      case None =>
        throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data */
  def unregisterShuffle(shuffleId: Int) {
    shuffleStatuses.remove(shuffleId).foreach { shuffleStatus =>
      shuffleStatus.invalidateSerializedMapOutputStatusCache()
    }
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    shuffleStatuses.valuesIterator.foreach { _.removeOutputsOnExecutor(execId) }
    incrementEpoch()
  }

  /** Check if the given shuffle is being tracked */
  def containsShuffle(shuffleId: Int): Boolean = shuffleStatuses.contains(shuffleId)

  def getNumAvailableOutputs(shuffleId: Int): Int = {
    shuffleStatuses.get(shuffleId).map(_.numAvailableOutputs).getOrElse(0)
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed), or None
   * if the MapOutputTrackerMaster doesn't know about this shuffle.
   */
  def findMissingPartitions(shuffleId: Int): Option[Seq[Int]] = {
    shuffleStatuses.get(shuffleId).map(_.findMissingPartitions())
  }

  /**
   * Return statistics about all of the outputs for a given shuffle.
   */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    shuffleStatuses(dep.shuffleId).withMapStatuses { statuses =>
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      for (s <- statuses) {
        for (i <- 0 until totalSizes.length) {
          totalSizes(i) += s.getSizeForBlock(i)
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
  }

  /**
   * Return the preferred hosts on which to run the given map output partition in a given shuffle,
   * i.e. the nodes that the most outputs for that partition are on.
   *
   * @param dep shuffle dependency object
   * @param partitionId map output partition that we want to read
   * @return a sequence of host names
   */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
      : Seq[String] = {
    // shuffleLocalityEnabled默认true
    // SHUFFLE_PREF_MAP_THRESHOLD默认=1000
    // SHUFFLE_PREF_REDUCE_THRESHOLD默认=1000
    // REDUCER_PREF_LOCS_FRACTION=0.2
    if (shuffleLocalityEnabled && dep.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD &&
        dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      // 这里会过滤清洗出满足要求的所有BlockManagerId
      // 补充：BlockManager在每个Executor和Drvier中都存在唯一一个负责数据的传输，接收和持久化,在之后的章节会介绍
      val blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
        dep.partitioner.numPartitions, REDUCER_PREF_LOCS_FRACTION)
      if (blockManagerIds.nonEmpty) {
        // 拿到所有BlockManager的host地址
        blockManagerIds.get.map(_.host)
      } else {
        Nil
      }
    } else {
      Nil
    }
  }

  /**
   * Return a list of locations that each have fraction of map output greater than the specified
   * threshold.
   *
   * @param shuffleId id of the shuffle
   * @param reducerId id of the reduce task
   * @param numReducers total number of reducers in the shuffle
   * @param fractionThreshold fraction of total map output size that a location must have
   *                          for it to be considered large.
   */
  def getLocationsWithLargestOutputs(
      shuffleId: Int,
      reducerId: Int,
      numReducers: Int,
      fractionThreshold: Double)
    : Option[Array[BlockManagerId]] = {
    // 拿到这个shuffleId对应的shuffleStatuses,这个会再注册shuffle的时候生成
    // 而里面的mapstatus是在shuffleMapTask完成好了后生成(map output)
    val shuffleStatus = shuffleStatuses.get(shuffleId).orNull
    if (shuffleStatus != null) {
      // 里面主要封装了synchronized用作访问这个shuffle中的mapStatuses数组的线程安全
      // 补充下 ：在创建一个ShuffleMapStage的时候就会把自己注册到Driver端的MapOutputTrackerMaster上
      // 然后同时里面也会生成对应的shuffleStatus和一个分区对应一个mapStatus

      // 里面主要是两种类型：①CompressedMapStatus ②HighlyCompressedMapStatus
      shuffleStatus.withMapStatuses { statuses =>
        if (statuses.nonEmpty) {
          // HashMap to add up sizes of all blocks at the same location
          // Map里面存放的是相同地址BlockManagerId对应的所有blocks大小
          val locs = new HashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          var mapIdx = 0
          // 里面会遍历出所有的mapStatus
          while (mapIdx < statuses.length) {
            // 从第一个mapStatu开始拿取
            val status = statuses(mapIdx)
            // status may be null here if we are called between registerShuffle, which creates an
            // array with null entries for each output, and registerMapOutputs, which populates it
            // with valid status entries. This is possible if one thread schedules a job which
            // depends on an RDD which is currently being computed by another thread.
            // 在registerShuffle的时候status可能会变成null，所以这里加了个判断
            if (status != null) {
              // 提取并解压缩block,默认是压缩的
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) {
                // 提取对应的BlockManagerId的blockSize并把刚刚解压缩的block大小叠加进去
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                // 叠加到总输出中
                totalOutputSize += blockSize
              }
            }
            // 开始遍历下个mapStatus
            mapIdx = mapIdx + 1
          }
          val topLocs = locs.filter { case (loc, size) =>
            // 过滤条件是：当前blockManager的block总大小 / 所有block大小 >= 0.2(默认)
            // 如果为true就说明当前blockManager的block实在太多了，若果再把tasks
            // 分配到这个blockManager的话就很可能造成性能瓶颈，比如说等待延迟调度等
            size.toDouble / totalOutputSize >= fractionThreshold
          }
          // Return if we have any locations which satisfy the required threshold
          if (topLocs.nonEmpty) {
            // 返回满足要求的BlockManagerId的数组
            return Some(topLocs.keys.toArray)
          }
        }
      }
    }
    None
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  // This method is only called in local-mode.
  def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    shuffleStatuses.get(shuffleId) match {
      case Some (shuffleStatus) =>
        shuffleStatus.withMapStatuses { statuses =>
          MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
        }
      case None =>
        Seq.empty
    }
  }

  override def stop() {
    mapOutputRequests.offer(PoisonPill)
    threadpool.shutdown()
    sendTracker(StopMapOutputTracker)
    trackerEndpoint = null
    shuffleStatuses.clear()
  }
}

/**
 * Executor-side client for fetching map output info from the driver's MapOutputTrackerMaster.
 * Note that this is not used in local-mode; instead, local-mode Executors access the
 * MapOutputTrackerMaster directly (which is possible because the master and worker share a comon
 * superclass).
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {

  val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]().asScala

  /** Remembers which map output locations are currently being fetched on an executor. */
  private val fetching = new HashSet[Int]

  override def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
      : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    val statuses = getStatuses(shuffleId)
    try {
      MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
    } catch {
      case e: MetadataFetchFailedException =>
        // We experienced a fetch failure so our mapStatuses cache is outdated; clear it:
        mapStatuses.clear()
        throw e
    }
  }

  /**
   * Get or fetch the array of MapStatuses for a given shuffle ID. NOTE: clients MUST synchronize
   * on this array when reading it, because on the driver, we may be changing it in place.
   *
   * (It would be nice to remove this restriction in the future.)
   */
  private def getStatuses(shuffleId: Int): Array[MapStatus] = {
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTime = System.currentTimeMillis
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        while (fetching.contains(shuffleId)) {
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the statuses; do so
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        fetchedStatuses
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      statuses
    }
  }


  /** Unregister shuffle data. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * because of a fetch failure. Each executor task calls this with the latest epoch
   * number on the driver at the time it was created.
   */
  def updateEpoch(newEpoch: Long): Unit = {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }
}

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: Array[MapStatus], broadcastManager: BroadcastManager,
      isLocal: Boolean, minBroadcastSize: Int): (Array[Byte], Broadcast[Array[Byte]]) = {
    val out = new ByteArrayOutputStream
    out.write(DIRECT)
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val arr = out.toByteArray
    if (arr.length >= minBroadcastSize) {
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // toByteArray creates copy, so we can reuse out
      out.reset()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(new GZIPOutputStream(out))
      oos.writeObject(bcast)
      oos.close()
      val outArr = out.toByteArray
      logInfo("Broadcast mapstatuses size = " + outArr.length + ", actual size = " + arr.length)
      (outArr, bcast)
    } else {
      (arr, null)
    }
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    assert (bytes.length > 0)

    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val objIn = new ObjectInputStream(new GZIPInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[Array[MapStatus]]
      case BROADCAST =>
        // deserialize the Broadcast, pull .value array out of it, and then deserialize that
        val bcast = deserializeObject(bytes, 1, bytes.length - 1).
          asInstanceOf[Broadcast[Array[Byte]]]
        logInfo("Broadcast mapstatuses size = " + bytes.length +
          ", actual size = " + bcast.value.length)
        // Important - ignore the DIRECT tag ! Start from offset 1
        deserializeObject(bcast.value, 1, bcast.value.length - 1).asInstanceOf[Array[MapStatus]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * Given an array of map statuses and a range of map output partitions, returns a sequence that,
   * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
   * stored at that block manager.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param startPartition Start of map output partition ID range (included in range)
   * @param endPartition End of map output partition ID range (excluded from range)
   * @param statuses List of map statuses, indexed by map ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   */
  def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      statuses: Array[MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
      } else {
        for (part <- startPartition until endPartition) {
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
        }
      }
    }

    splitsByAddress.toSeq
  }
}