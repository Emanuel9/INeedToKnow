package com.emi.ineed.logger

import java.text.SimpleDateFormat
import java.io.StringWriter
import java.io.PrintWriter

import com.emi.ineed.utils.ApplicationExecutionMetrics
import com.google.gson.Gson

object LoggersUtil {

  val DateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val OneMb = 1024 * 1024
  val OneSecond = 1000

  private val gson = new Gson

  /**
    * This method is used convert remote exceptions into text
    */
  def stackTraceToString(exception: Throwable): String = {
    val textWriter = new StringWriter
    exception.printStackTrace(new PrintWriter(textWriter))
    textWriter.close
    textWriter.toString
  }

  /**
    * returns application name and id taken from execution metrics
    */
  def appIdentifier(): String = {
    s"${ApplicationExecutionMetrics.appName}:${ApplicationExecutionMetrics.appId}"
  }

  def toJson(value: Any): String = {
    gson.toJson(value)
  }
}
