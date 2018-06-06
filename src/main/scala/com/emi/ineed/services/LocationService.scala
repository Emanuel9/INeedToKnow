package com.emi.ineed.services

import com.koddi.geocoder._
import com.emi.ineed.config.LocationConfig._


object LocationService {

  def getLocationByStreet(streetName : String) : Unit = {

    val geo = Geocoder.create(ApiKey)

    val results = geo.lookup(streetName)
    val location = results.head.geometry.location


    println(s"Latitude: ${location.latitude}, Longitude: ${location.longitude}")
  }


}
