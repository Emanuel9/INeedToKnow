package com.emi.ineed.services

import com.koddi.geocoder._
import com.emi.ineed.config.LocationConfig._


object LocationService {

  def getLocationByStreet(streetName : String): LocationData = {

    val geo = Geocoder.create(ApiKey)
    val results = geo.lookup(streetName)
    val location = results.head.geometry.location
    val addressComponents : Seq[AddressComponent] = results.head.addressComponents


    var latitude = location.latitude
    var longitude = location.longitude
    var streetNumber = ""
    var route = ""
    var city= ""
    var country = ""
    var postal_code = ""

    for ( addressComponent <- addressComponents ) {
      if (addressComponent.types.contains("route")) route = addressComponent.longName
      if (addressComponent.types.contains("street_number")) streetNumber = addressComponent.longName
      if (addressComponent.types.contains("locality")) {
        city = addressComponent.longName match {
          case "București" => "Bucharest"
          case _ => addressComponent.longName
        }
      }
      if (addressComponent.types.contains("country")) country = addressComponent.longName
      if (addressComponent.types.contains("postal_code")) postal_code = addressComponent.longName
    }

    LocationData(route, streetNumber, city, country, postal_code, latitude, longitude)
  }
}

case class LocationData(route: String, streetNumber: String, city: String, country: String, postal_code: String, latitude: Double, longitude: Double )
