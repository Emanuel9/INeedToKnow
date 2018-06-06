package com.emi.ineed.logger

import org.slf4j.LoggerFactory

trait ErrorLogger {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def writeError(message: String, exception: Throwable ): Unit = {
    val stackTrace = LoggersUtil.stackTraceToString(exception)
    val logErrorMsg = s"$message\n$stackTrace"
    logger.error(logErrorMsg)
  }
}

