package com.emi.ineed.crawling.safety

import com.emi.ineed.services.LocationData
import com.emi.ineed.utils.{Crawler, StreetSchema}
import net.ruippeixotog.scalascraper.scraper.ContentExtractors.elementList
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.Element
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s.native.Json

class SafetyCrawling extends Crawler {

  var safetyUrl = config.getString("crime_safety.base")
  val category = "safety"

  def startCrawling(locationData: LocationData): Seq[StreetSchema] = {
    var retrievedData: Seq[StreetSchema] = Seq[StreetSchema]()

    safetyUrl = safetyUrl.replace("{city}", locationData.city.toString)
    val items = browser.get(safetyUrl) >?> elementList("table.table_builder_with_value_explanation")
    items match {
      case Some(items: List[Element]) => {
        val safetyItem = items.tail.head
        val tableSafety = safetyItem >?> element("tbody")
        val tableSafetyDetails = (tableSafety >?> elementList("tr")).head.head
        val kpiElements = collection.mutable.Map[String, String]()

        tableSafetyDetails.foreach( item => {
          var safetyKPIName = ""
          val safetyKPINameElement = item >?> element("td.columnWithName") match {
            case Some(safetyKPINameElement) => safetyKPIName = safetyKPINameElement.text
            case None =>
          }
          var safetyKPIValue = ""
          val safetyKpiValueElement = item >?> element("td.indexValueTd") match {
            case Some(safetyKpiValueElement)  => safetyKPIValue = safetyKpiValueElement.text
            case None =>
          }
          kpiElements.put(safetyKPIName, safetyKPIValue)
        } )

        val timestamp = DateTime.now().toString()
        val content = Json(DefaultFormats).write(kpiElements)
        val entity = StreetSchema(locationData.route,category,timestamp,content, locationData.latitude, locationData.longitude)
        retrievedData = retrievedData :+ entity
      }
      case None =>

    }

    retrievedData
  }
}
