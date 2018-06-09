package com.emi.ineed.crawling.crime

import com.emi.ineed.services.LocationData
import com.emi.ineed.utils.{Crawler, StreetSchema}
import com.fasterxml.jackson.databind.ObjectMapper
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.Element
import org.joda.time.DateTime
import org.json4s.native.Json
import org.json4s.DefaultFormats

class CrimeCrawling extends Crawler {

  var crimeEventsUrl = config.getString("crime_safety.base")
  val category = "crime"
  val mapper = new ObjectMapper()

  def startCrawling(locationData: LocationData): Seq[StreetSchema] = {
    var retrievedData: Seq[StreetSchema] = Seq[StreetSchema]()

    crimeEventsUrl = crimeEventsUrl.replace("{city}", locationData.city.toString)
    val items = browser.get(crimeEventsUrl) >?> elementList("table.table_builder_with_value_explanation")
    items match {
      case Some(items: List[Element]) => {
        val crimeItem = items.head
        val tableCrime = crimeItem >?> element("tbody")
        val tableCrimeDetails = (tableCrime >?> elementList("tr")).head.head
        val kpiElements = collection.mutable.Map[String, String]()
        tableCrimeDetails.foreach( item => {
          var crimeKPIName = ""
          val crimeKPINameElement = item >?> element("td.columnWithName") match {
            case Some(crimeKPINameElement) => crimeKPIName = crimeKPINameElement.text
            case None =>
          }
          var crimeKPIValue = ""
          val crimeKpiValueElement = item >?> element("td.indexValueTd") match {
            case Some(crimeKpiValueElement)  => crimeKPIValue = crimeKpiValueElement.text
            case None =>
          }
          kpiElements.put(crimeKPIName, crimeKPIValue)
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
