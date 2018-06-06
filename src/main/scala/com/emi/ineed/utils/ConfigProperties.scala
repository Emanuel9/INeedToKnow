package com.emi.ineed.utils

import org.apache.spark.sql.SparkSession

object ConfigProperties extends Enumeration {

  type ConfigProperties = Value

  val StreetDataKeyspace : Value = Value("spark.cassandracluster.streetDataKeyspace")
  val StreetDataSchemaHeader: Value = Value("spark.streetdata.schema")

  def getProperty(sparkSession: SparkSession, propertyName: ConfigProperties): String = {
    sparkSession.conf.get(propertyName.toString)
  }
}
