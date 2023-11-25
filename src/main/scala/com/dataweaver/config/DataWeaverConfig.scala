package com.dataweaver.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Try}

class DataWeaverConfig(config: Config) {
  /**
   * Retrieves the path to the pipelines directory from the provided configuration.
   *
   * @return The path to the pipelines directory.
   */
  def getPipelinesDir: String = {
    config.getString("weaver.pipeline.pipelines_dir")
  }

  /**
   * Retrieves the path to the JAR file containing the DataWeaver application.
   *
   * @return The path to the JAR file.
   */
  def getWaverJarPath: String = {
    config.getString("weaver.pipeline.jar_path")
  }

  /**
   * Retrieves the cluster URL from the provided configuration.
   *
   * @return The cluster URL.
   */
  def getClusterUrl: String = {
    config.getString("weaver.pipeline..cluster_url")
  }
}

object DataWeaverConfig {
  /**
   * Attempts to load the application configuration from the specified file.
   *
   * @param configPath
   * @return A `Try` containing the loaded configuration or an exception if there was an error.
   */
  def load(configPath: String): Try[DataWeaverConfig] = {
    Try {
      val config = ConfigFactory.parseFile(new java.io.File(s"$configPath/application.conf"))

      new DataWeaverConfig(config)
    } recoverWith {
      case e: Exception => Failure(new Exception("Error loading project configuration: " + e.getMessage))
    }
  }
}