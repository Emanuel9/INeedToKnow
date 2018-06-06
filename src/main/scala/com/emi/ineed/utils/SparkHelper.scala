package com.emi.ineed.utils

import org.apache.spark.sql.SparkSession

trait SparkHelper {

  var sparkSession: SparkSession = _

  def initSpark(appName: String): SparkSession = {
    sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.cassandra.connection.host","127.0.0.1")
      .config("spark.extraListeners", "com.emi.ineed.utils.CustomSparkListener")
      .config("spark.cassandracluster.streetDataKeyspace","ineedtoknow")
      .config("spark.streetdata.schema","street,category,import_date,description,latitude,longitude")
      .getOrCreate()

    sparkSession
  }

  def closeSpark() : Unit = {
    sparkSession.close()
  }
}

