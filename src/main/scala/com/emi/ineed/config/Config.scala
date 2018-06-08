package com.emi.ineed.config

import com.typesafe.config.ConfigFactory

trait Config {
  val config = ConfigFactory.load()
}

class ConfigWrapper extends Config