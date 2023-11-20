package com.dataweaver.config

import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.Paths
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
    val configPath = Paths.get(System.getProperty("user.dir"), "weaver_project.conf").toString
    ConfigFactory.parseFile(new java.io.File(configPath))
  }

  /**
   * Retrieves the path to the pipelines directory from the provided configuration.
   *
   * @param config The application configuration.
   * @return The path to the pipelines directory.
   */
  def getPipelinesDir(config: Config): String = {
    config.getString("weaver.pipelines_dir")
  }

  /**
   * Retrieves the default runner from the provided configuration.
   *
   * @param config The application configuration.
   * @return The default runner.
   */
  def getDefaultRunner(config: Config): String = {
    config.getString("weaver.default_runner")
  }

}