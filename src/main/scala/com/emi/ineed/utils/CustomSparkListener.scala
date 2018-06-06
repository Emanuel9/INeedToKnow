package com.emi.ineed.utils

import org.apache.spark.ExceptionFailure
import org.apache.spark.executor._
import org.apache.spark.scheduler._

import scala.collection.mutable.{HashMap, ListBuffer}

final case class CustomTaskInfo(taskId: Long, index: Int, attemptNumber: Int,
                                successful: Boolean, failed: Boolean, killed: Boolean)

final case class TasksExecutionStats(totalTasksFailed: Int, distinctTasksFailed: Int, tasksKilled: Int,
                                     tasksSuccessful: Int, tasksSuccessfulAfterAttempts: Int,
                                     totalExecutedTasks: Int, distinctExecutedTasks: Int)

/**
  * A collection of numbers that represents metrics about reading data from external systems.
  */
class CustomInputMetrics {

  /**
    * Total number of bytes read.
    */
  private var _bytesRead = 0L

  /**
    * Total number of records read.
    */
  private var _recordsRead = 0L

  private[utils] def addMetrics(inputMetrics: InputMetrics): Unit = {
    _bytesRead += inputMetrics.bytesRead
    _recordsRead += inputMetrics.recordsRead
  }

  def bytesRead: Long = _bytesRead
  def recordsRead: Long = _recordsRead

  override def toString: String = s"inputMetrics: {bytesRead:${_bytesRead},  recordsRead:${_recordsRead}}"
}

/**
  * A collection of numbers that represents metrics about writing data to external systems.
  */
class CustomOutputMetrics {

  /**
    * Total number of bytes written.
    */
  private var _bytesWritten = 0L

  /**
    * Total number of records written.
    */
  private var _recordsWritten = 0L

  private[utils] def addMetrics(outputMetrics: OutputMetrics): Unit = {
    _bytesWritten += outputMetrics.bytesWritten
    _recordsWritten += outputMetrics.recordsWritten
  }

  def bytesWritten: Long = _bytesWritten
  def recordsWritten: Long = _recordsWritten

  override def toString: String = s"outputMetrics: {bytesWritten:${_bytesWritten},  recordsWritten:${_recordsWritten}}"
}

class CustomShuffleReadMetrics {
  /**
    * Number of remote blocks fetched in all shuffles by all tasks.
    */
  private var _remoteBlocksFetched = 0L

  /**
    * Number of local blocks fetched in all shuffles by all tasks.
    */
  private var _localBlocksFetched = 0L

  /**
    * Total number of remote bytes read from the all shuffles by all tasks.
    */
  private var _remoteBytesRead = 0L

  /**
    * Shuffle data sum that was read from the local disk (as opposed to from a remote executor).
    */
  private var _localBytesRead = 0L

  /**
    * Time the tasks spent waiting for remote shuffle blocks. This only includes the time
    * blocking on shuffle input data. For instance if block B is being fetched while the task is
    * still not finished processing block A, it is not considered to be blocking on block B.
    */
  private var _fetchWaitTime = 0L

  /**
    * Total number of records read from all shuffles by all tasks.
    */
  private var _recordsRead = 0L

  private[utils] def addMetrics(shuffleReadMetrics: ShuffleReadMetrics): Unit = {
    _remoteBlocksFetched += shuffleReadMetrics.remoteBlocksFetched
    _localBlocksFetched += shuffleReadMetrics.localBlocksFetched
    _remoteBytesRead += shuffleReadMetrics.remoteBytesRead

    _localBytesRead += shuffleReadMetrics.localBytesRead
    _fetchWaitTime += shuffleReadMetrics.fetchWaitTime
    _recordsRead += shuffleReadMetrics.recordsRead
  }

  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def localBlocksFetched: Long = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead

  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead

  /**
    * Total bytes fetched in all shuffles by all tasks (both remote and local).
    */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
    * Number of blocks fetched in all shuffles by all tasks (remote and local).
    */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  override def toString: String = s"""shuffleReadMetrics: {remoteBlocksFetched:${_remoteBlocksFetched},
            localBlocksFetched:${_localBlocksFetched}, remoteBytesRead:${_remoteBytesRead},
            localBytesRead:${_localBytesRead},  fetchWaitTime:${_fetchWaitTime}, recordsRead:${_recordsRead},
            totalBytesRead:${totalBytesRead}, totalBlocksFetched:${totalBlocksFetched}}"""
}

class CustomShuffleWriteMetrics {

  /**
    * Number of bytes written for the shuffle by all tasks.
    */
  private var _bytesWritten = 0L

  /**
    * Total number of records written to the shuffle by all tasks.
    */
  private var _recordsWritten = 0L

  /**
    * Time spent blocking on writes to disk or buffer cache, in nanoseconds.
    */
  private var _writeTime = 0L

  private[utils] def addMetrics(shuffleWriteMetrics: ShuffleWriteMetrics): Unit = {
    _bytesWritten += shuffleWriteMetrics.bytesWritten
    _recordsWritten += shuffleWriteMetrics.recordsWritten
    _writeTime += shuffleWriteMetrics.writeTime
  }

  def bytesWritten: Long = _bytesWritten
  def recordsWritten: Long = _recordsWritten
  def writeTime: Long = _writeTime

  override def toString: String = s"shuffleWriteMetrics: {bytesWritten:${_bytesWritten},  recordsWritten:${_recordsWritten},  writeTime:${_writeTime}}"
}

object ApplicationExecutionMetrics {

  private var _appEndedSuccessful = false

  private var _appName = ""
  private var _appId = ""

  private var _appStartTime = 0L
  private var _appEndTime = 0L

  private var _stagesNo = 0L ///no used for the moment
  private var _tasksNo = 0L

  private val _tasksEndErrors = ListBuffer[ExceptionFailure]()

  /**
    * Tasks are organized by stage & index
    */
  private val _tasksStatus = HashMap[Int, HashMap[Int, ListBuffer[CustomTaskInfo]]]()

  /**
    * Time taken on the executor to deserialize all tasks.
    */
  private var _executorsDeserializeTime = 0L

  /**
    * Time the executors spend running all tasks (including fetching shuffle data).
    */
  private var _executorsRunTime = 0L

  /**
    * The number of bytes all tasks transmitted back to the driver as the TaskResult.
    */
  private var _resultSize = 0L

  /**
    * Amount of time the JVM spent in garbage collection while executing all tasks.
    */
  private var _jvmGCTime = 0L

  /**
    * Amount of time spent serializing the tasks result.
    */
  private var _resultSerializationTime = 0L

  /**
    * The number of in-memory bytes spilled by all tasks.
    */
  private var _memoryBytesSpilled = 0L

  /**
    * The number of on-disk bytes spilled by all tasks.
    */
  private var _diskBytesSpilled = 0L

  /**
    * Peak memory used by internal data structures created during shuffles, aggregations and
    * joins. The value of this  should be approximately the sum of the peak sizes
    * across all such data structures created in all tasks. For SQL jobs, this only tracks all
    * unsafe operators and ExternalSort.
    */
  private var _peakExecutionMemory = 0L

  private var _tasksDuration = 0L

  private val _inputMetrics = new CustomInputMetrics
  private val _outputMetrics = new CustomOutputMetrics
  private val _shuffleReadMetrics = new CustomShuffleReadMetrics
  private val _shuffleWriteMetrics = new CustomShuffleWriteMetrics

  private[utils] def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    _appStartTime = applicationStart.time
    _appName = applicationStart.appName
    _appId = applicationStart.appId.getOrElse("no-app-id")
  }

  private[utils] def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    addMetrics(taskEnd.taskMetrics)

    val reason = taskEnd.reason
    reason match {
      case e: ExceptionFailure => _tasksEndErrors += e
      case _ =>
    }

    _tasksNo += 1
    _tasksDuration += taskEnd.taskInfo.duration

    collectTasksStatusInfo(taskEnd)
  }

  def collectTasksStatusInfo(taskEnd: SparkListenerTaskEnd) {
    val taskStageId = taskEnd.stageId

    val taskInfo: TaskInfo = taskEnd.taskInfo
    val taskIndex = taskInfo.index

    var tasksForStage = HashMap[Int, ListBuffer[CustomTaskInfo]]()
    var customTaskInfoList = ListBuffer[CustomTaskInfo]()

    if (!_tasksStatus.contains(taskStageId)) {

      tasksForStage(taskIndex) = customTaskInfoList
      _tasksStatus(taskStageId) = tasksForStage

    } else {
      tasksForStage = _tasksStatus(taskStageId)

      if (!tasksForStage.contains(taskIndex)) {
        customTaskInfoList = ListBuffer[CustomTaskInfo]()
        tasksForStage(taskIndex) = customTaskInfoList
      } else {
        customTaskInfoList = tasksForStage(taskIndex)
      }
    }

    val customTaskInfo = CustomTaskInfo(taskInfo.taskId, taskIndex, taskInfo.attemptNumber,
      taskInfo.successful, taskInfo.failed, taskInfo.killed)
    customTaskInfoList += customTaskInfo
  }

  private[utils] def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    _appEndTime = applicationEnd.time
  }

  private def addMetrics(taskMetrics: TaskMetrics): Unit = {
    if (Option(taskMetrics).isDefined) {
      _executorsDeserializeTime += taskMetrics.executorDeserializeTime
      _executorsRunTime += taskMetrics.executorRunTime
      _resultSize += taskMetrics.resultSize

      _jvmGCTime += taskMetrics.jvmGCTime
      _resultSerializationTime += taskMetrics.resultSerializationTime
      _memoryBytesSpilled += taskMetrics.memoryBytesSpilled

      _diskBytesSpilled += taskMetrics.diskBytesSpilled
      _peakExecutionMemory = Math.max(taskMetrics.peakExecutionMemory, _peakExecutionMemory)

      _inputMetrics.addMetrics(taskMetrics.inputMetrics)
      _outputMetrics.addMetrics(taskMetrics.outputMetrics)
      _shuffleReadMetrics.addMetrics(taskMetrics.shuffleReadMetrics)
      _shuffleWriteMetrics.addMetrics(taskMetrics.shuffleWriteMetrics)
    }
  }

  def signalAppEndedSuccessful(): Unit = {
    if (_appEndedSuccessful) {
      throw new RuntimeException("Could not signal twice app end")
    }
    _appEndedSuccessful = true
  }

  def appEndedSuccessful: Boolean = _appEndedSuccessful

  def executorsDeserializeTime: Long = _executorsDeserializeTime
  def executorsRunTime: Long = _executorsRunTime
  def resultSize: Long = _resultSize
  def jvmGCTime: Long = _jvmGCTime
  def resultSerializationTime: Long = _resultSerializationTime
  def memoryBytesSpilled: Long = _memoryBytesSpilled
  def diskBytesSpilled: Long = _diskBytesSpilled
  def peakExecutionMemory: Long = _peakExecutionMemory
  def tasksDuration: Long = _tasksDuration

  def inputMetrics: CustomInputMetrics = _inputMetrics
  def outputMetrics: CustomOutputMetrics = _outputMetrics
  def shuffleReadMetrics: CustomShuffleReadMetrics = _shuffleReadMetrics
  def shuffleWriteMetrics: CustomShuffleWriteMetrics = _shuffleWriteMetrics

  def appName: String = _appName
  def appId: String = _appId

  def appExecutionTime: Long = _appEndTime - _appStartTime
  def appStartTime: Long = _appStartTime
  def appEndTime: Long = _appEndTime
  def tasksEndErrors: ListBuffer[ExceptionFailure] = _tasksEndErrors

  def tasksStatusDetails: HashMap[Int, HashMap[Int, ListBuffer[CustomTaskInfo]]] = _tasksStatus

  def tasksExecutionStatistics(): TasksExecutionStats = {
    var totalTasksFailed = 0
    var distinctTasksFailed = 0
    var tasksKilled = 0
    var tasksSuccessful = 0
    var tasksSuccessfulAfterAttempts = 0
    var totalExecutedTasks = 0
    var distinctExecutedTasks = 0

    for ((stageId, tasksForStage) <- _tasksStatus) {
      distinctExecutedTasks += tasksForStage.size

      for ((index, tasksInfoList) <- tasksForStage) {
        totalExecutedTasks += tasksInfoList.size
        /**
          * both variable are related with task index inside a stage
          */
        var isTaskSuccessful = false
        var hasTaskFailedAttempts = false

        for (customTaskInfo <- tasksInfoList) {
          /**
            * if a task has some fails -> success is for last attempts
            */
          if (customTaskInfo.successful) {
            isTaskSuccessful = true
          } else if (customTaskInfo.failed) {
            totalTasksFailed += 1
            hasTaskFailedAttempts = true
          } else if (customTaskInfo.killed) {
            tasksKilled += 1
          }
        }

        if (hasTaskFailedAttempts) {
          distinctTasksFailed += 1
        }

        if (isTaskSuccessful) {
          tasksSuccessful += 1

          if (hasTaskFailedAttempts) {
            tasksSuccessfulAfterAttempts += 1
          }
        }
      }
    }

    TasksExecutionStats(totalTasksFailed, distinctTasksFailed, tasksKilled,
      tasksSuccessful, tasksSuccessfulAfterAttempts, totalExecutedTasks, distinctExecutedTasks)
  }

  override def toString: String = s"""ApplicationMetrics:{
     appName:${_appName},
     appId:${_appId},
     appExecutionTime:${appExecutionTime},
     appStartTime:${appStartTime},
     appEndTime:${appEndTime},
     executedTasks:${_tasksNo},
     tasksNoOfErrors:${_tasksEndErrors.size},
     executorsDeserializeTime:${_executorsDeserializeTime},
     executorsRunTime:${_executorsRunTime},
     resultSize:${_resultSize},
     jvmGCTime:${_jvmGCTime},
     resultSerializationTime:${_resultSerializationTime},
     memoryBytesSpilled:${_memoryBytesSpilled},
     diskBytesSpilled:${_diskBytesSpilled},
     peakExecutionMemory:${_peakExecutionMemory},
     tasksDuration:${_tasksDuration},

     ${_inputMetrics},

     ${_outputMetrics},

     ${_shuffleReadMetrics},

     ${_shuffleWriteMetrics},

     $tasksExecutionStatistics
    }"""
}

class CustomSparkListener extends SparkListener {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    ApplicationExecutionMetrics.onApplicationStart(applicationStart)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    ApplicationExecutionMetrics.onTaskEnd(taskEnd)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    ApplicationExecutionMetrics.onApplicationEnd(applicationEnd)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {

  }
}


