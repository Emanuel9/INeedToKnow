import java.time.LocalDateTime

import com.emi.ineed.utils._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import com.datastax.spark.connector._
import com.datastax.spark.connector.types.CassandraOption.Value
import com.emi.ineed.crawling.around.AroundCrawling
import com.emi.ineed.crawling.crime.CrimeCrawling
import com.emi.ineed.crawling.happiness.HappinessCrawling
import com.emi.ineed.crawling.safety.SafetyCrawling
import com.emi.ineed.crawling.social.SocialCrawling
import com.emi.ineed.services.LocationData
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.concurrent.ExecutionContextExecutorService
import com.emi.ineed.utils.CassandraDataFrameHelper._
import com.emi.ineed.services.LocationService._
object StreetInfoApplication extends SparkApplicationWithLogging {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private implicit var executionContext : ExecutionContextExecutorService = _

  def triggerSparkJob(sparkSession: SparkSession, streetName: String, category: String): Unit = {
//    val locationData = LocationData("Strada Academiei", "14", "Bucharest", "Romania", "010014", 44.4351979, 26.0996322   )
    val locationData = getLocationByStreet(streetName)

    val streetInfoData = CrawlerCategory.withName(category) match {
      case CrawlerCategory.AROUND => new AroundCrawling().startCrawling(locationData)
      case CrawlerCategory.CRIME => new CrimeCrawling().startCrawling(locationData)
      case CrawlerCategory.HAPPINESS => new HappinessCrawling().startCrawling(locationData)
      case CrawlerCategory.SAFETY => new SafetyCrawling().startCrawling(locationData)
      case CrawlerCategory.SOCIAL => new SocialCrawling().startCrawling(locationData)
      case _ => throw new Exception(s"Couldn't match ${category} category with any existing ones!")
    }

    val sc = sparkSession.sparkContext

    val rdd = sc.parallelize(streetInfoData)
      .map( x => Row( x.streetName, x.category, x.timestamp, x.description, x.latitude, x.longitude ))

    val schemaHeaderString = ConfigProperties.getProperty(sparkSession, ConfigProperties.StreetDataSchemaHeader)
    val schema = buildSchema(schemaHeaderString)
    val df = sparkSession.sqlContext.createDataFrame(rdd, schema)

    writeStreetInfo(df)
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
      SaveMode.Append)
  }
}
