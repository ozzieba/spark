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


package org.apache.spark.streaming.scheduler

import java.util.Properties

import org.apache.spark.metrics.source.Source

import scala.util.Random
import org.apache.spark.{ExecutorAllocationClient, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark.streaming.StreamingContext
import com.codahale.metrics.{Gauge, MetricRegistry}
import kafka.utils.ZookeeperClient
import org.apache.spark.SparkEnv
import org.apache.spark.streaming.util.RecurringTimer

/**
  * Class that manages executor allocated to a StreamingContext, and dynamically request or kill
  * executors based on the statistics of the streaming computation. This is different from the core
  * dynamic allocation policy; the core policy relies on executors being idle for a while, but the
  * micro-batch model of streaming prevents any particular executors from being idle for a long
  * time. Instead, the measure of "idle-ness" needs to be based on the time taken to process
  * each batch.
  *
  * At a high level, the policy implemented by this class is as follows:
  * - Use StreamingListener interface get batch processing times of completed batches
  * - Periodically take the average batch completion times and compare with the batch interval
  * - If (avg. proc. time / batch interval) >= scaling up ratio, then request more executors.
  *   The number of executors requested is based on the ratio = (avg. proc. time / batch interval).
  * - If (avg. proc. time / batch interval) <= scaling down ratio, then try to kill an executor that
  *   is not running a receiver.
  *
  * This features should ideally be used in conjunction with backpressure, as backpressure ensures
  * system stability, while executors are being readjusted.
  */
private[streaming] class DVExecutorAllocationManager(
                                                      client: ExecutorAllocationClient,
                                                      ssc: StreamingContext) extends StreamingListener with Logging {
  import DVExecutorAllocationManager._
  private val conf = ssc.conf
  private val batchDurationMs = ssc.graph.batchDuration.milliseconds
  private val scalingUpRatio = conf.getDouble(SCALING_UP_RATIO_KEY, SCALING_UP_RATIO_DEFAULT)
  private val scalingDownRatio = conf.getDouble(SCALING_DOWN_RATIO_KEY, SCALING_DOWN_RATIO_DEFAULT)
  private val scalingUpMaxRatio = conf.getDouble(SCALING_UP_MAX_RATIO_KEY, SCALING_UP_MAX_RATIO_DEFAULT)
  private val scalingDownMinRatio = conf.getDouble(SCALING_DOWN_MIN_RATIO_KEY, SCALING_DOWN_MIN_RATIO_DEFAULT)
  private val minNumExecutors = conf.getInt(MIN_EXECUTORS_KEY,2)
  private val maxNumExecutors = conf.getInt(MAX_EXECUTORS_KEY, Integer.MAX_VALUE)
  private var latestExecutors: Int = client.getExecutorIds().size
  private val proportional = conf.getDouble(PROPORTIONAL_KEY, PROPORTIONAL_DEFAULT)
  private val integral = conf.getDouble(INTEGRAL_KEY, INTEGRAL_DEFAULT)
  private val derivative = conf.getDouble(DERIVATIVE_KEY, DERIVATIVE_DEFAULT)
  private val derivativeMax = conf.getDouble(DERIVATIVE_MAX_KEY, DERIVATIVE_MAX_DEFAULT)
  private val processingRateHalfLifeSec = conf.getDouble(PROCESSING_RATE_HALF_LIFE_KEY, PROCESSING_RATE_HALF_LIFE_DEFAULT)
  private val zkPath = conf.get(ZK_PATH_KEY,"/ndp/youtube/apps/"+conf.get("spark.app.name")+"/executors")
  private var targetTotalExecutors = latestExecutors
  private val clock: Clock = new SystemClock
  private var onBatchStartTime : Long = -1
  private var batchDuration : Long = 0
  private var numBatches : Long = 0
  private var timer_started: Boolean = false
  private val timer = new RecurringTimer(clock, 60 * 1000,
    _ => reviveExecutors(), "streaming-revive-executor")

  lazy val getZKParams: Properties = {
    val props = new Properties()
    props.setProperty("group.id", conf.get("spark.app.name"))
    props.setProperty("zookeeper.connect", conf.get("spark.dv.zk.servers"))
    props
  }
  validateSettings()
  val metricsSource = new DVExecutorAllocationManagerSource
  SparkEnv.get.metricsSystem.registerSource(metricsSource)

  def start(): Unit = {
    if(timer_started == false){
      timer.start()
      timer_started = true
      logInfo("Done starting time")
    }
    logInfo(s"DVExecutorAllocationManager started with " +
      s"ratios = [$scalingUpRatio, $scalingDownRatio]")
  }

  def stop(): Unit = {
    timer.stop(interruptTimer = true)
    timer_started = false
    logInfo("DVExecutorAllocationManager stopped")
  }


  /**
    * Make sure executors have actually been requested, and haven't all died since last manageAllocation()
    */
  private def reviveExecutors(): Unit = synchronized {
      logInfo(s"Checking if spark needs revive")
      latestExecutors = ssc.sparkContext.getExecutorStorageStatus.length - 1
      logInfo(s"latestExecutors: ${latestExecutors}, targetTotalExecutor: ${targetTotalExecutors}")
      if(latestExecutors < targetTotalExecutors) {
        logInfo(s"Reviving spark !!")
        client.requestTotalExecutors(targetTotalExecutors, 0 ,Map.empty)
      }
  }


  /**
    * Manage executor allocation by requesting or killing executors based on the collected
    * batch statistics.
    */
  private def manageAllocation(): Unit = synchronized {
    if(APPLICATIONDEAD){
      ssc.scheduler.listenerBus.removeListener(this)
      logInfo(s"Killing ${client.getExecutorIds().size} Executors because I am suicidal")
      client.requestTotalExecutors(0,0, Map.empty)
      client.killExecutors(client.getExecutorIds)
      return
    }

    val processingRate = expProcessingRate
    //error is how much we need to increase processing rate to keep up with input rate, in messages/s
    val error = latestInputRate - processingRate
    //historical error is the number of messages/s we would need to increase the processing rate to process the backlog in one batch
    val historicalError = getLag() * 1000 / batchDurationMs
    //dError is the expected change in the inputRate over a single batch, again in messages/s
    //derivativeMax by default limits the effect of the derivative to 20% of the input rate
    val dError = math.min(dInputRate * batchDurationMs / 1000 , derivativeMax * latestInputRate )

    logInfo(s"""
               | DVExecutorAllocationManager latestExecutors = $latestExecutors,
               | DVExecutorAllocationManager error = $error, proportional = $proportional,
               | DVExecutorAllocationManager historicalError = $historicalError, integral = $integral,
               | DVExecutorAllocationManager dError = $dError, derivative =  $derivative,
               | DVExecutorAllocationManager latestProcessingRate = $latestProcessingRate
               | DVExecutorAllocationManager expProcessingRatePerExecutor = $expProcessingRatePerExecutor
               | DVExecutorAllocationManager expProcessingRate = $expProcessingRate
               | DVExecutorAllocationManager processingRate = $processingRate
               | DVExecutorAllocationManager latestInputRate = $latestInputRate
            """.stripMargin)
    var scaleRatio = 1.0
    
    if (processingRate != 0) {
      scaleRatio = (processingRate + proportional * error
        + integral * historicalError
        + derivative * dError) / processingRate
    }
    logInfo(s"scaleRatio: $scaleRatio" )

    var newNumExecutors = latestExecutors
    if (scaleRatio > 1.0) {
      newNumExecutors += math.round(scalingUpRatio * (math.min(scaleRatio, scalingUpMaxRatio) - 1.0) * latestExecutors).toInt
      logInfo(s"Requesting  executors to reach target $newNumExecutors executors")
    } else {
      newNumExecutors += math.round(scalingDownRatio * (math.max(scaleRatio, scalingDownMinRatio) - 1.0) * latestExecutors).toInt
      logInfo(s"Killing executors to reach target $newNumExecutors executors")
    }
    setNumExecutors(newNumExecutors)
  }


  /** Request the specified number of executors over the currently active one */
  private def setNumExecutors(newNumExecutors: Int): Unit = {
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")
    targetTotalExecutors =
      math.max(math.min(maxNumExecutors, newNumExecutors), minNumExecutors)
    if (targetTotalExecutors > latestExecutors) {
      client.requestTotalExecutors(targetTotalExecutors, 0, Map.empty)
      logInfo(s"Requested total $targetTotalExecutors executors")
    }
    else
      killExecutors(targetTotalExecutors)
  }

  /** Kill multiple executors to reach target number */
  private def killExecutors(newNumExecutors: Int): Unit = {
    for ( a <- newNumExecutors until latestExecutors){
      killExecutor()
    }
  }
  /** Kill an executor that is not running any receiver, if possible */
  private def killExecutor(): Unit = {
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")

    if (allExecIds.nonEmpty && allExecIds.size > minNumExecutors) {
      val execIdsWithReceivers = ssc.scheduler.receiverTracker.allocatedExecutors.values.flatten.toSeq
      logInfo(s"Executors with receivers (${execIdsWithReceivers.size}): ${execIdsWithReceivers}")

      val removableExecIds = allExecIds.diff(execIdsWithReceivers)
      logDebug(s"Removable executors (${removableExecIds.size}): ${removableExecIds}")
      if (removableExecIds.nonEmpty) {
        val execIdToRemove = removableExecIds(Random.nextInt(removableExecIds.size))
        client.killExecutor(execIdToRemove)
        logInfo(s"Requested to kill executor $execIdToRemove")
      } else {
        logInfo(s"No non-receiver executors to kill")
      }
    } else {
      logInfo("No available executor to kill")
    }
  }


  private def validateSettings(): Unit = {

    require(
      scalingUpRatio > 0,
      s"Config $SCALING_UP_RATIO_KEY must be more than 0")

    require(
      scalingDownRatio > 0,
      s"Config $SCALING_DOWN_RATIO_KEY must be more than 0")

    require(
      minNumExecutors > 0,
      s"Config $MIN_EXECUTORS_KEY must be more than 0")

    require(
      maxNumExecutors > 0,
      s"$MAX_EXECUTORS_KEY must be more than 0")

    require(
      scalingUpRatio > scalingDownRatio,
      s"Config $SCALING_UP_RATIO_KEY must be more than config $SCALING_DOWN_RATIO_KEY")

    if (conf.contains(MIN_EXECUTORS_KEY) && conf.contains(MAX_EXECUTORS_KEY)) {
      require(
        maxNumExecutors >= minNumExecutors,
        s"Config $MAX_EXECUTORS_KEY must be more than config $MIN_EXECUTORS_KEY")
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    start()
    onBatchStartTime = System.currentTimeMillis / 1000
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    start()
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    stop()
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    onBatchStartTime = -1
    numBatches += 1
    if (!batchCompleted.batchInfo.outputOperationInfos.values.exists(_.failureReason.nonEmpty)) {
      latestExecutors = ssc.sparkContext.getExecutorStorageStatus.length - 1
      ZookeeperClient.writeCustomConfig(getZKParams,zkPath,latestExecutors.toString())
      latestProcessingRate = batchCompleted.batchInfo.numRecords * 1000 /  batchCompleted.batchInfo.processingDelay.sum
      // Using modified EWMA to smooth processing rate
      // see https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
      // also, since generally processing rate per executor should be stable don't let it change by more than a small amount each time
      // the allowed change per batch is calculated to allow factor of 2 change after processingRateHalfLifeSec
      // by default processingRateHalfLifeSec = 60 min, so we are allowed to change by a factor of 0.5^(1/60)=98.85% to 2^(1/60) = 101.16% each 1-minute batch
      // this means we can double or halve the processing rate over one hour
      // For the first few batches we are allowed to change more quickly, ie by a factor of 2/numBatches, until this becomes smaller than the above
      // finally, we assume that we are always capable of processing 100 messages/s/executor, even if the last batch says otherwise because of low traffic
      val maxProcessingRateChange = (1.0 + 2.0 / numBatches)
        .max(scala.math.pow(2,batchDurationMs / (processingRateHalfLifeSec * 1000)))
      expProcessingRatePerExecutor =  (expProcessingRatePerExecutor + 0.5 * (latestProcessingRate / latestExecutors - expProcessingRatePerExecutor))
        .min(expProcessingRatePerExecutor * maxProcessingRateChange)
        .max(expProcessingRatePerExecutor / maxProcessingRateChange)
      expProcessingRatePerExecutor = expProcessingRatePerExecutor.max(100.0)
      expProcessingRate = expProcessingRatePerExecutor * latestExecutors
      manageAllocation()
//      stop()
    }
    else {
      logInfo("not running dynamic Allocation since batch had failures")
    }
  }
  private[spark] class DVExecutorAllocationManagerSource extends Source {
    val sourceName = "DVExecutorAllocationManager"
    val metricRegistry = new MetricRegistry()

    private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
    }

    registerGauge("latestExecutors", latestExecutors, 0)
    registerGauge("latestInputRate", latestInputRate, 0)
    registerGauge("latestProcessingRate", latestProcessingRate, 0)
    registerGauge("expProcessingRate", expProcessingRate, 0)
    registerGauge("expProcessingRatePerExecutor", expProcessingRatePerExecutor, 0)
    registerGauge("latestLag", getLag(), 0)
    registerGauge("dInputRate", dInputRate, 0)
    registerGauge("targetTotalExecutors", targetTotalExecutors, 0)
  }
}

object DVExecutorAllocationManager extends Logging {
  val ENABLED_KEY = "spark.streaming.dv.dynamicAllocation.enabled"


  val SCALING_UP_RATIO_KEY = "spark.streaming.dv.dynamicAllocation.scalingUpRatio"
  val SCALING_UP_RATIO_DEFAULT = 1.0

  val SCALING_UP_MAX_RATIO_KEY = "spark.streaming.dv.dynamicAllocation.scalingUpMaxRatio"
  val SCALING_UP_MAX_RATIO_DEFAULT = 2.0

  val SCALING_DOWN_RATIO_KEY = "spark.streaming.dv.dynamicAllocation.scalingDownRatio"
  val SCALING_DOWN_RATIO_DEFAULT = 0.5

  val SCALING_DOWN_MIN_RATIO_KEY = "spark.streaming.dv.dynamicAllocation.scalingDownMinRatio"
  val SCALING_DOWN_MIN_RATIO_DEFAULT = 0.5

  val PROPORTIONAL_KEY = "spark.streaming.dv.dynamicAllocation.proportional"
  val PROPORTIONAL_DEFAULT = 1.0

  val INTEGRAL_KEY = "spark.streaming.dv.dynamicAllocation.integral"
  val INTEGRAL_DEFAULT = 0.2

  val DERIVATIVE_KEY = "spark.streaming.dv.dynamicAllocation.derivative"
  val DERIVATIVE_DEFAULT = 0.5

  val DERIVATIVE_MAX_KEY = "spark.streaming.dv.dynamicAllocation.derivative.max"
  val DERIVATIVE_MAX_DEFAULT = 0.2

  val MIN_EXECUTORS_KEY = "spark.streaming.dv.dynamicAllocation.minExecutors"

  val MAX_EXECUTORS_KEY = "spark.streaming.dv.dynamicAllocation.maxExecutors"
  val ZK_PATH_KEY = "spark.dv.zk.executors.path"

  val PROCESSING_RATE_HALF_LIFE_KEY = "spark.streaming.dv.dynamicAllocation.processingRateHalfLifeSec"
  val PROCESSING_RATE_HALF_LIFE_DEFAULT = 3600


  def isDVDynamicAllocationEnabled(conf: SparkConf): Boolean = {
    val streamingDynamicAllocationEnabled = conf.getBoolean(ENABLED_KEY, false)
    val testing = conf.getBoolean("spark.streaming.dv.dynamicAllocation.testing", false)
    streamingDynamicAllocationEnabled && (!Utils.isLocalMaster(conf) || testing)
  }
  private var latestInputRate: Double = 0
  private var latestProcessingRate: Double = 0
  private var latestProcessingRatePerExecutor: Double = 0
  private var expProcessingRate: Double = 100
  private var expProcessingRatePerExecutor: Double = 100
  private var updateNumber: Int = 1
  private var _leaderOffset: Long = -1L
  private var _currentOffset: Long = -1L
  private var dInputRate: Double = 0
  private var APPLICATIONDEAD = false

  def leaderOffset = _leaderOffset
  def currentOffset = _currentOffset
  def leaderOffset_= (value:Long):Unit = {
    val now = System.currentTimeMillis()
    val newLatestInputRate = 1000 * (value - _leaderOffset)/(now - leaderOffsetUpdateTime)
    if (newLatestInputRate == 0 && now - leaderOffsetUpdateTime < 300000) { // input batch may be a bit delayed
      return
    }
    if (updateNumber > 1) { // first update we don't have have enough info for input rate calculation
      latestInputRate = newLatestInputRate
      if (updateNumber > 2) { // second update we can update input Rate but not dInputRate
        dInputRate = 1000 * (newLatestInputRate - latestInputRate) / (now - leaderOffsetUpdateTime)
      }
      else {
        updateNumber += 1
      }
    }
    else {
      updateNumber += 1
    }
    _leaderOffset = value
    leaderOffsetUpdateTime = now
  }
  def currentOffset_= (value:Long):Unit = {
    val now = System.currentTimeMillis()
    _currentOffset = value
    currentOffsetUpdateTime = now
  }
  def getLag() = {
    val now = System.currentTimeMillis()
    val projectedLeaderOffset = leaderOffset + latestInputRate * (now - leaderOffsetUpdateTime ) / 1000
    val projectedCurrentOffset = currentOffset + expProcessingRate * (now - currentOffsetUpdateTime) / 1000
    Math.max(projectedLeaderOffset - projectedCurrentOffset,0)
  }

  var leaderOffsetUpdateTime: Long = -1L
  var currentOffsetUpdateTime: Long = -1L

  def stop_application(): Unit ={
    APPLICATIONDEAD = true
  }
  def createIfEnabled(ssc:StreamingContext): Option[DVExecutorAllocationManager] = {
    if (isDVDynamicAllocationEnabled(ssc.conf)) {
      val executorAllocClient: ExecutorAllocationClient = ssc.sparkContext.schedulerBackend match {
        case b: ExecutorAllocationClient => b.asInstanceOf[ExecutorAllocationClient]
        case _ => null
      }
      Some(new DVExecutorAllocationManager(executorAllocClient, ssc))
    } else None
  }
}


