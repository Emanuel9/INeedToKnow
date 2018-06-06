package com.emi.ineed.logger

import org.slf4j.LoggerFactory
import com.emi.ineed.utils.ApplicationExecutionMetrics
import java.util.Date

import com.google.gson.annotations.SerializedName

import collection.JavaConverters._

import com.google.gson.internal.LinkedTreeMap

/**
  * TODO: Smarter solution are the following
  * 1) https://github.com/mp911de/logstash-gelf
  * 2) https://logging.apache.org/log4j/2.x/manual/layouts.html
  * 3) http://logging.paluch.biz/examples/log4j-2.x.html - this requires logs update
  * 4) http://logging.paluch.biz/examples/log4j-1.2.x-json.html - this is possible with current logs
  * 5) configure Spark logs not app logs and everything is unified & clear
  */

private class InputMetrics {

  private val totalDataRead = ApplicationExecutionMetrics.inputMetrics.bytesRead / LoggersUtil.OneMb

  private[logger] var totalRecordsRead = ApplicationExecutionMetrics.inputMetrics.recordsRead
}

private class OutputMetrics {

  private var totalDataWritten = ApplicationExecutionMetrics.outputMetrics.bytesWritten / LoggersUtil.OneMb

  private[logger] var totalRecordsWritten = ApplicationExecutionMetrics.outputMetrics.recordsWritten
}

private class ShuffleReadMetrics {

  /**
    * Total fetch time in seconds for all shuffle operations
    */
  private val totalFetchWaitTime = ApplicationExecutionMetrics.shuffleReadMetrics.fetchWaitTime / LoggersUtil.OneSecond

  /**
    * Total remote data read by shuffle operations in MB
    */
  private val totalRemoteRead = ApplicationExecutionMetrics.shuffleReadMetrics.remoteBytesRead / LoggersUtil.OneMb

  /**
    * Total local data read by shuffle operations in MB
    */
  private val totalLocalRead = ApplicationExecutionMetrics.shuffleReadMetrics.localBytesRead / LoggersUtil.OneMb

  /**
    * Total data fetched in all shuffles by all tasks (both remote and local).
    */
  private val totalDataRead = ApplicationExecutionMetrics.shuffleReadMetrics.totalBytesRead / LoggersUtil.OneMb

  private val totalBlocksFetched = ApplicationExecutionMetrics.shuffleReadMetrics.totalBlocksFetched

  private val totalRecordsRead = ApplicationExecutionMetrics.shuffleReadMetrics.recordsRead
}

private class ShuffleWriteMetrics {

  /**
    * Total data written by shuffle operations in MB
    */
  private val totalDataWritten = ApplicationExecutionMetrics.shuffleWriteMetrics.bytesWritten / LoggersUtil.OneMb

  /**
    * Total write time in seconds for all shuffle operations
    */
  private val totalWriteTime = ApplicationExecutionMetrics.shuffleWriteMetrics.writeTime / (LoggersUtil.OneSecond * 1000000)

  private val totalRecordsWritten = ApplicationExecutionMetrics.shuffleWriteMetrics.recordsWritten
}

private class MonitoringEvent {

  @SerializedName("@timestamp")
  private val timestamp = LoggersUtil.DateFormatter.format(new Date)

  private val applicationName = ApplicationExecutionMetrics.appName

  private val applicationId = ApplicationExecutionMetrics.appId

  private val appStartTime = LoggersUtil.DateFormatter.format(new Date(ApplicationExecutionMetrics.appStartTime))

  private val appEndTime = LoggersUtil.DateFormatter.format(new Date(ApplicationExecutionMetrics.appEndTime))

  /**
    * Execution time as seconds for executed application in seconds
    */
  private val appExecutionTime = ApplicationExecutionMetrics.appExecutionTime / LoggersUtil.OneSecond

  /**
    * Result time serialization in seconds
    */
  private val resultSerializationTime = ApplicationExecutionMetrics.resultSerializationTime / LoggersUtil.OneSecond

  /**
    * Result size in MB
    */
  private val resultSize = ApplicationExecutionMetrics.resultSize / LoggersUtil.OneMb

  /**
    * Sum of all tasks execution time in seconds
    */
  private val totalTasksDuration = ApplicationExecutionMetrics.tasksDuration / LoggersUtil.OneSecond

  /**
    * Amount of time the JVMs spent in garbage collection while executing all tasks, measured in seconds.
    */
  private val totalJvmGCTime = ApplicationExecutionMetrics.jvmGCTime / LoggersUtil.OneSecond

  /**
    * The number of in-memory bytes spilled by this all tasks in MB.
    */
  private val totalMemoryBytesSpilled = ApplicationExecutionMetrics.memoryBytesSpilled / LoggersUtil.OneMb

  private val maxShufflesPeakExecutionMemory = ApplicationExecutionMetrics.peakExecutionMemory / LoggersUtil.OneMb

  private val totalTasksErrors = ApplicationExecutionMetrics.tasksEndErrors.size

  private val executorsDeserializeTime = ApplicationExecutionMetrics.executorsDeserializeTime / LoggersUtil.OneSecond

  private val executorsRunTime = ApplicationExecutionMetrics.executorsRunTime / LoggersUtil.OneSecond

  private val totalDiskBytesSpilled = ApplicationExecutionMetrics.diskBytesSpilled / LoggersUtil.OneMb

  private val inputMetrics = new InputMetrics

  private val outputMetrics = new OutputMetrics

  private val shuffleReadMetrics = new ShuffleReadMetrics

  private val shuffleWriteMetrics = new ShuffleWriteMetrics

  private val tasksExecutionStats = ApplicationExecutionMetrics.tasksExecutionStatistics

  private val recordsReadPerSecond = inputMetrics.totalRecordsRead / appExecutionTime

  private val recordsWrittenPerSecond = outputMetrics.totalRecordsWritten / appExecutionTime

  private[logger] var applicationArgs = new LinkedTreeMap[String, String]()

  private var applicationStatusReason = "OK"

  private val applicationStatusOk = {
    val hasAppTasks = (tasksExecutionStats.totalExecutedTasks > 0)
    val allTasksOk = (tasksExecutionStats.distinctExecutedTasks == tasksExecutionStats.tasksSuccessful)
    val tasksNotKilled = (tasksExecutionStats.tasksKilled == 0)

    if (!hasAppTasks) {
      applicationStatusReason = "APP_HAS_NO_TASKS"
    } else if (!tasksNotKilled) {
      applicationStatusReason = "SOME_TASKS_KILLED"
    } else if (!allTasksOk) {
      applicationStatusReason = "NOT_ALL_TASKS_SUCCESSFUL"
    } else if (!ApplicationExecutionMetrics.appEndedSuccessful) {
      applicationStatusReason = "APP_ENDED_UNEXPECTEDLY"
    }

    val isApplicationOk = hasAppTasks && allTasksOk &&
      tasksNotKilled && ApplicationExecutionMetrics.appEndedSuccessful

    isApplicationOk
  }
}

trait MonitoringLogger {

  private val logger = LoggerFactory.getLogger("monitoringLogger")

  def writeMonitoringAppMetrics(appParams: Map[String, String]): Unit = {
    val event = new MonitoringEvent

    if (appParams.nonEmpty) {
      event.applicationArgs.putAll(appParams.asJava)
    }
    logger.info(LoggersUtil.toJson(event))
  }
}

