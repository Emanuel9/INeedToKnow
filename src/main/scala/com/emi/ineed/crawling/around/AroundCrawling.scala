package com.emi.ineed.crawling.around

import com.emi.ineed.services.LocationData
import com.emi.ineed.utils.{Crawler, StreetSchema}
import dispatch._
import Defaults._
import com.emi.ineed.config.LocationConfig._
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.slf4j.LoggerFactory
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{ read }
import org.json4s.NoTypeHints

class AroundCrawling extends Crawler{

  private val logger = LoggerFactory.getLogger(this.getClass)

  case class Location(lat: Double, lng: Double)
  case class Viewport(northeast: Location, southwest: Location)
  case class Geometry(location: Location)
  case class RunningStatus(open_now: Boolean, weekday_text: List[String])
  case class Results(geometry: Geometry, id: Option[String], name: Option[String])
  case class RootJsonObject(results: List[Results])

  val category = "around"

  def startCrawling(locationData: LocationData): Seq[StreetSchema] = {
    var retrievedData: Seq[StreetSchema] = Seq[StreetSchema]()

    val request = url(ApiUrl)
    val requestAsGet = request.GET

    val builtRequest = requestAsGet
        .addQueryParameter("key", ApiKey)
        .addQueryParameter("location", s"${locationData.latitude},${locationData.longitude}")
        .addQueryParameter("radius", "3000")
        .addQueryParameter("sensor", "false")

    val content = Http(builtRequest)

    content onSuccess {
      case case_ok if case_ok.getStatusCode() == 200 => {
          val contentToAdd = handleJsonOutput(case_ok.getResponseBody)
          contentToAdd.results.foreach( result => {
              val timestamp = DateTime.now().toString()
              val data = Map("id" -> result.id.get, "name" -> result.name.get )
              val contentToAddToDB = Json(DefaultFormats).write(data)
              val entity = StreetSchema(locationData.route,category,timestamp,contentToAddToDB,result.geometry.location.lat, result.geometry.location.lng)
              retrievedData = retrievedData :+ entity
          })
        }
      case case_not_ok =>
        logger.error("Failed with status code" + case_not_ok.getStatusCode())
    }

    content onFailure {
      case failure =>
        logger.error(failure.getMessage)
    }

    retrievedData
  }

  def handleJsonOutput(responseBody: String): RootJsonObject = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val output = read[RootJsonObject](responseBody)
    output
  }
}
