package com.emi.ineed.logger

import com.emi.ineed.utils.ApplicationExecutionMetrics
import org.slf4j.LoggerFactory

trait MetricsLogger extends MonitoringLogger {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def writeAppMetrics(appParams: Map[String,String]): Unit = {

    val metrics = ApplicationExecutionMetrics
    logger.info(metrics.toString())

    if (metrics.tasksEndErrors.size > 0) {
      for (error <- metrics.tasksEndErrors) {
        val logErrorMsg = s"App Execution Errors: ${LoggersUtil.appIdentifier}:\n${error.fullStackTrace}"
        logger.error(logErrorMsg)
      }
    }

    writeMonitoringAppMetrics(appParams)
  }
}
