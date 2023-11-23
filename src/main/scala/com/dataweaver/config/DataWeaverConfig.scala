package com.dataweaver.config

import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.Paths
import scala.util.{Failure, Try}

class DataWeaverConfig(config: Config) {
  /**
   * Retrieves the path to the pipelines directory from the provided configuration.
   *
   * @return The path to the pipelines directory.
   */
  def getPipelinesDir: String = {
    config.getString("weaver.pipelines_dir")
  }

  /**
   * Retrieves the default runner from the provided configuration.
   *
   * @return The default runner.
   */
  def getDefaultRunner: String = {
    config.getString("weaver.default_runner")
  }

  /**
   * Retrieves the path to the JAR file containing the DataWeaver application.
   *
   * @return The path to the JAR file.
   */
  def getWaverJarPath: String = {
    config.getString("weaver.jar_path")
  }

  /**
   * Retrieves the cluster URL from the provided configuration.
   *
   * @return The cluster URL.
   */
  def getClusterUrl: String = {
    config.getString("weaver.cluster_url")
  }

  /**
   * Retrieves the path to the directory containing pipeline files.
   *
   * @return The path to the directory containing pipeline files.
   */
  def getPipelineDir: String = {
    config.getString("weaver.pipeline_dir")
  }
}

object DataWeaverConfig {
  /**
   * Attempts to load the application configuration from the "weaver_project.conf" file.
   *
   * @return A `Try` containing the loaded configuration or an exception if there was an error.
   */
  def load(): Try[DataWeaverConfig] = {
    Try {
      val configPath = Paths.get(System.getProperty("user.dir"), "application.conf").toString
      val config = ConfigFactory.parseFile(new java.io.File(configPath))

      new DataWeaverConfig(config)
    } recoverWith {
      case e: Exception => Failure(new Exception("Error loading project configuration: " + e.getMessage))
    }
  }
}