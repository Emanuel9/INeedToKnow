package com.emi.ineed.crawling.around

import com.emi.ineed.config.LocationConfig._
import dispatch.{Http, url}

object AroundCrawlingPlaces {

  def findUsingGoogleMaps(): Unit = {
    val request = url("https://maps.googleapis.com/maps/api/place/nearbysearch/json")
    val requestAsGet = request.GET //not required but lets be explicit

    //Step 2 : Set the required parameters
    val builtRequest = requestAsGet.addQueryParameter("key", ApiKey)
      .addQueryParameter("location", "40.5556204,-74.4162536")
      .addQueryParameter("radius", "10000")
      .addQueryParameter("sensor", "false")
      .addQueryParameter("keyword", "starbucks")

    //Step 3: Make the request (method is already set above)
//    val content = Http(builtRequest)
//
//    //Step 4: Once the response is available
//    //response completed successfully
//    content onSuccess {
//
//      //Step 5 : Request was successful & response was OK
//      case x if x.getStatusCode() == 200 =>
//        //Step 6 : Response was OK, read the contents
//        handleJsonOutput(x.getResponseBody)
//      case y => //Step 7 : Response is not OK, read the error
//        println("Failed with status code" + y.getStatusCode())
//    }
//
//    //Step 7 : Request did not complete successfully, read the error
//    content onFailure {
//      case x =>
//        println("Failed but"); println(x.getMessage)
//    }
  }

  //CASE CLASSES
  case class Location(lat: Double, lng: Double)
  case class Geometry(location: Location)
  case class RunningStatus(open_now: Boolean)
  case class Results(geometry: Geometry, icon: Option[String], id: Option[String], name: Option[String],
                     opening_hours: Option[RunningStatus], price_level: Option[Double], rating: Option[Double],
                     reference: Option[String], types: Option[List[String]], vicinity: Option[String])
  case class RootJsonObject(results: List[Results])

  //JSON imports
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization.{ read }
  import org.json4s.NoTypeHints

  private def handleJsonOutput(body: String): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)

    val output = read[RootJsonObject](body)
    println(s"Total Results: ${output.results.size}")

    for (each <- output.results)
      println(s"each.name.getat{each.vicinity.get}")
  }
}
