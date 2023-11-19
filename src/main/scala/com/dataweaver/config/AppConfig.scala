package com.dataweaver.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
 * A utility object for loading and retrieving application configuration settings.
 */
object AppConfig {
  /**
   * Attempts to load the application configuration from the "flow.conf" file.
   *
   * @return A `Try` containing the loaded configuration or an exception if there was an error.
   */
  def loadConfig: Try[Config] = Try {
    val configPath = System.getProperty("user.dir") + "/config/flow.conf"
    ConfigFactory.parseFile(new java.io.File(configPath))
  }

  /**
   * Retrieves the Spark master URL from the provided configuration.
   *
   * @param config The application configuration.
   * @return The Spark master URL.
   */
  def getSparkMaster(config: Config): String = {
    config.getString("spark.master")
  }

}