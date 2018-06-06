import java.time.LocalDateTime

import com.emi.ineed.utils.{ConfigProperties, SparkApplicationWithLogging, TableNames}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import com.datastax.spark.connector._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.concurrent.ExecutionContextExecutorService
import com.emi.ineed.utils.CassandraDataFrameHelper._

object StreetInfoApplication extends SparkApplicationWithLogging {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private implicit var executionContext : ExecutionContextExecutorService = _

  def triggerSparkJob(sparkSession: SparkSession, streetName: String, category: String): Unit = {
    val sc = sparkSession.sparkContext

    val nowString = LocalDateTime.now().toString()

    val streetSocialInfoTest: Array[(String, String, String, String, Double, Double)] =
      Array(
        (streetName, category, nowString, "A description1", 41.21, 41.4),
        ("Batistei", category, nowString, "A description2", 42.21, 42.4),
        ("Frumoasa", category, nowString, "A description3", 43.21, 43.4),
        ("Urata", category, nowString, "A description4", 44.21, 44.4),
        ("Bulevardul Regina Elisabeta", category, nowString, "A description5", 45.21, 45.4)
      )

    val rdd = sc.parallelize(streetSocialInfoTest)
      .map( x => Row( x._1, x._2, x._3, x._4, x._5, x._6 ))

    val schemaHeaderString = ConfigProperties.getProperty(sparkSession, ConfigProperties.StreetDataSchemaHeader)
    val schema = buildSchema(schemaHeaderString)
    val df = sparkSession.sqlContext.createDataFrame(rdd, schema)

    df.show()
    writeStreetInfo(df)

//    getLocationByStreet("Splaiul Independentei 204")
  }

  override def afterSparkJob(): Unit = {
    if (Option(executionContext).isDefined) {
      executionContext.shutdown()
    }
  }

  def main(args: Array[String]): Unit = {
    mainSparkJob(args)
  }

  def writeStreetInfo( streetData: DataFrame ): Unit = {
    writeToTable(streetData,
      TableNames.StreetData,
      ConfigProperties.StreetDataKeyspace,
      SaveMode.Overwrite)
  }
}
