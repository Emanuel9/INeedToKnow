package com.emi.ineed.utils

import com.emi.ineed.config.Config
import com.emi.ineed.services.LocationData
import net.ruippeixotog.scalascraper.browser.JsoupBrowser

trait Crawler extends Config {

  val browser = JsoupBrowser()

  def startCrawling(locationData: LocationData): Seq[StreetSchema]

}