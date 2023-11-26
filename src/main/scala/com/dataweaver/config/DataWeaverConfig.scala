package com.dataweaver.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.LogManager

import scala.sys.process._
import scala.util.Try

class DataWeaverConfig(config: Config) {

  /** Retrieves the path to the pipelines directory from the provided configuration.
    *
    * @return
    *   The path to the pipelines directory.
    */
  def getPipelinesDir: String = {
    config.getString("weaver.pipeline.pipelines_dir")
  }

  /** Retrieves the path to the JAR file containing the DataWeaver application.
    *
    * @return
    *   The path to the JAR file.
    */
  def getWaverJarPath: String = {
    config.getString("weaver.pipeline.jar_path")
  }

  /** Retrieves the cluster URL from the provided configuration.
    *
    * @return
    *   The cluster URL.
    */
  def getClusterUrl: String = {
    config.getString("weaver.pipeline.cluster_url")
  }
}

object DataWeaverConfig {

  private val logger = LogManager.getLogger(getClass)

  /** Attempts to load the application configuration from the specified file.
    *
    * @param configPath
    * @return
    *   A `Try` containing the loaded configuration or an exception if there was an error.
    */
  def load(configPath: String): Try[DataWeaverConfig] = {
    Try {
      val currentDirectory = "pwd".!!.trim
      logger.info(s"Current directory: $currentDirectory")

      val filesAndDirectories = "ls".!!.trim
      logger.info(s"Files and directories: $filesAndDirectories")

      val config: Config = Try(ConfigFactory.parseResources(s"$configPath/application.conf"))
        .filter(_.entrySet().size() > 0)
        .recoverWith { case _: Exception =>
          logger.warn(
            s"Could not find application.conf in resources. Attempting to load from file system."
          )
          Try(ConfigFactory.parseFile(new java.io.File(s"$configPath/application.conf")))
        }
        .get

      logger.info(s"Loaded config: ${config.toString}")
      new DataWeaverConfig(config)
    }.recover { case e: Exception =>
      throw new Exception(s"Error loading configuration: ${e.getMessage}")
    }
  }
}
