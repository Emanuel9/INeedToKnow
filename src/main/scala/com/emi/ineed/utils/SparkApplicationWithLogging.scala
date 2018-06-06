package com.emi.ineed.utils

import com.emi.ineed.logger.{ErrorLogger, MetricsLogger}
import org.apache.spark.sql.SparkSession
import org.slf4j.{LoggerFactory, MDC}

trait SparkApplicationWithLogging extends SparkHelper with MetricsLogger with ErrorLogger {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val IntputParamStreet = "street"
  val InputParamCategory = "category"

  var parametersMap:Map[String, String] = Map[String, String]()

  def triggerSparkJob(sparkSession: SparkSession, streetName: String, categoryName: String)

  def checkJobLineArgumentsPassed(appParams: Map[String,String]): (Option[String], Option[String]) = {
    try {
      val streetName = parametersMap(IntputParamStreet)
      val categoryName = parametersMap(InputParamCategory)
      (Some(streetName), Some(categoryName))
    } catch {
      case exception : NoSuchElementException => {
        logger.error(s"Required parameter --street <street>  or --category <category> for params $appParams not found. Exception occurred: ${exception.getMessage}")
        (None, None)
      }
    }
  }

  def getParams(args: Array[String]): Map[String, String] = {
    var parameters: Map[String, String] = Map()

    if (args.length == 0) {
      throw new Exception("Wrong parameters format - must have street and category parameters, with first parameter starting with '--'  ")
    }

    parameters += (args(0).substring(2) -> args(1))
    parameters += (args(2).substring(2) -> args(3))
    parameters
  }

  def mainSparkJob(args: Array[String]): Unit = {
    parametersMap = getParams(args)
    val (streetOption, categoryOption) = checkJobLineArgumentsPassed(parametersMap)
    val jobName = getClass.getSimpleName.replace("$", "")

    (streetOption, categoryOption) match {
      case (Some(street), Some(category)) => {
        try {
          initSpark(s"${jobName}_${street}_${category}")

          MDC.put("appIdentifier", ApplicationExecutionMetrics.appName + ":" + ApplicationExecutionMetrics.appId)
          logger.info(s"Execution started for Spark job $jobName with street=${street} and category=${category}")
          triggerSparkJob(sparkSession, street, category)
          logger.info(s"Execution ended for Spark job $jobName with street=${street} and category=${category}")
          ApplicationExecutionMetrics.signalAppEndedSuccessful()
        } catch {
          case exception: Exception =>
            writeError(s"Unknown error, for params: $parametersMap", exception )
        } finally {
          closeSpark()
          writeAppMetrics(parametersMap)
          afterSparkJob()
          MDC.clear()
        }
      }

      case (None,None) | (Some(_),None) | (None, Some(_)) => logger.info("Required parameters street is missing!")
    }
  }

  def afterSparkJob(): Unit = {}
}

