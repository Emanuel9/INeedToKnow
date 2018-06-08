package com.emi.ineed.utils

object CrawlerCategory extends Enumeration {
  type CrawlingType = Value

  val AROUND = Value("around")
  val CRIME = Value("crime")
  val HAPPINESS = Value("happiness")
  val SAFETY = Value("safety")
  val SOCIAL = Value("social")
}
