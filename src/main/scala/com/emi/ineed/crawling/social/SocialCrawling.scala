package com.emi.ineed.crawling.social

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.FSM.Event
import com.emi.ineed.services.LocationData
import com.emi.ineed.utils.{Crawler, StreetSchema}
import org.joda.time.{DateTime, Days}
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.model.Element

class SocialCrawling extends Crawler {

  var socialEventsUrl = config.getString("social_events.base")
  val dateFormat = "yyyy-MM-dd"
  val category = "social"

  def startCrawling(locationData: LocationData): Seq[StreetSchema] = {
    var retrievedData: Seq[StreetSchema] = Seq[StreetSchema]()

    var eventTitle = ""
    var eventLocation = ""
    var eventShortDescription = ""
    var eventWillHappenOn = ""

    val start = DateTime.now
    val end   = DateTime.now.plusDays(20)
    val daysCount = Days.daysBetween(start, end).getDays()
    val period = (0 until daysCount).map(start.plusDays(_))

    for ( day <- period ) {
      val date = getDateInFormat(day.toDate)
      socialEventsUrl = socialEventsUrl.replace("{city}", locationData.city.toString)
      socialEventsUrl = socialEventsUrl.replace("{date}", date.toString)

      val items = browser.get(socialEventsUrl) >?> elementList("div.event-item")
      items match {
        case Some(items: List[Element]) => {
          items.foreach(item => {
            val nameElement = item >?> element("[property=name]") match {
              case Some(nameElement) => eventTitle = nameElement.text
              case None =>

            }
            val locationElement = item >?> element("[property=location]") match {
              case Some(locationElement) => eventLocation = locationElement.text
              case None =>

            }
            val shortDescriptionElement = item >?> element("p.short-desc") match {
              case Some(shortDescriptionElement) => eventShortDescription = shortDescriptionElement.text
              case None =>

            }

            val timeElement = item >?> element("span.time") match {
              case Some(timeElement) => eventWillHappenOn = timeElement.text
              case None =>

            }

            val timestamp = DateTime.now().toString()
            val content =s"${eventTitle}_${eventLocation}_${shortDescriptionElement}_${eventWillHappenOn}"


            val entity = StreetSchema(locationData.route,category,timestamp,content, locationData.latitude, locationData.longitude)
            retrievedData = retrievedData :+ entity
          })
        }
        case None =>
      }
    }

    retrievedData
  }

  def getDateInFormat(date: Date): String = {
    val simpleDataFormat  = new SimpleDateFormat(dateFormat)
    simpleDataFormat.format(date)
  }
}
