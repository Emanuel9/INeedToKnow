package com.emi.ineed.crawling.crime

import com.emi.ineed.services.LocationData
import com.emi.ineed.utils.{Crawler, StreetSchema}

class CrimeCrawling extends Crawler {

  def startCrawling(locationData: LocationData): Seq[StreetSchema] = ???
}
