package com.emi.ineed.utils

import com.emi.ineed.utils.ConfigProperties.ConfigProperties
import com.emi.ineed.utils.TableNames.TableNames
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * A kind of DAO
  */
object CassandraDataFrameHelper {

  private val CassandraName = "org.apache.spark.sql.cassandra"
  private val HeaderColumnDelimiter = ","

  var streetDataKeyspace : String =  _


  /**
    * Initialisation of the keyspace to use
    *
    * @param sqlContext
    */

  def initCassandraConnection(sqlContext: SQLContext): Unit = {
    streetDataKeyspace = sqlContext.getConf(ConfigProperties.StreetDataKeyspace.toString)
  }

  /**
    * Read from cassandra table. If there is no keyspace specified the default is set to reports keyspace.
    * If you want to read from other keyspace you need to specify the keyspace.
    * @param sqlContext
    * @param tableName -> need to specify the table.
    * @param keyspaceConfigProperty -> default ( reports keyspace )
    * @return
    */

  def readFromTable(sqlContext: SQLContext, tableName: TableNames, keyspaceConfigProperty:
  ConfigProperties = ConfigProperties.StreetDataKeyspace) : Dataset[Row] = {

    val keyspaceName = sqlContext.getConf(keyspaceConfigProperty.toString)
    sqlContext.read.format(CassandraName)
      .options(Map(
        "table" -> tableName.toString,
        "keyspace" -> keyspaceName,
        "spark.cassandra.input.split.size_in_mb" -> "128"))
      .load()
  }

  /**
    * write df to table "tableName" from keyspace "keyspaceName" with "writeMode"
    *
    * @param df
    * @param tableName
    * @param writeMode
    */
  def writeToTable(df: DataFrame, tableName: TableNames,
                   keyspaceConfigProperty: ConfigProperties = ConfigProperties.StreetDataKeyspace,
                   writeMode: SaveMode = SaveMode.Append): Unit = {
    val keyspaceName=df.sqlContext.getConf(keyspaceConfigProperty.toString)
    var options = Map("table" -> tableName.toString, "keyspace" -> keyspaceName)
    if ( writeMode.equals(SaveMode.Overwrite)) {
      options += ("confirm.truncate" -> "true")
    }

    df.write.format(CassandraName)
      .options( options )
      .mode(writeMode)
      .save()
  }

  def buildSchema(schemaString: String): StructType = {
    val fields = schemaString.split(HeaderColumnDelimiter)
      .map(fieldName => StructField(fieldName,
        if (fieldName.equals("latitude") || fieldName.equals("longitude") )
            DoubleType else StringType
        , nullable = true))
    val schema = StructType(fields)
    schema
  }
}

case class StreetSchema(streetName:String, category:String, timestamp:String, description:String, latitude:Double, longitude:Double)

