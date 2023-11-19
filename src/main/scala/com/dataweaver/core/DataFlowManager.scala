package com.dataweaver.core

import com.dataweaver.config.{AppConfig, DataPipelineConfig}
import com.dataweaver.parsing.YAMLParser
import com.dataweaver.pipeline.DataPipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Manages the execution of data flows defined in YAML configuration files.
 */
class DataFlowManager {
  private val logger = LoggerFactory.getLogger(getClass)
  private val maxRetries = 3 // Maximum number of retries

  /**
   * Executes data flows based on the specified tag and regex criteria.
   *
   * @param tag   Optional tag for filtering data flows.
   * @param regex Optional regular expression for filtering YAML files.
   */
  def executeDataFlows(tag: Option[String], regex: Option[String]): Unit = {
    implicit val spark = SparkSession.builder.appName("DataWeaverApp").getOrCreate()
    try {
      val yamlDirectory = loadYamlDirectory()
      val yamlFiles = Files.list(Paths.get(yamlDirectory))
        .toArray
        .map(_.asInstanceOf[java.nio.file.Path])
        .filter(path => regex.forall(r => path.toString.matches(r)) || regex.isEmpty)

      yamlFiles.foreach { path =>
        val content = Source.fromFile(path.toFile).mkString
        YAMLParser.parse(content).foreach { config =>
          if (tag.forall(config.tag.contains)) {
            runWithRetry(config, maxRetries)
          }
        }
      }
    } finally {
      spark.stop()
    }
  }

  private def loadYamlDirectory(): String = {
    val yamlDirectoryResult = AppConfig.loadConfig.flatMap { config =>
      Try(config.getString("yaml.directory"))
    }

    yamlDirectoryResult match {
      case Success(yamlDirectory) => yamlDirectory
      case Failure(ex) =>
        logger.error("Error loading 'yaml.directory' from configuration", ex)
        throw ex
    }
  }

  private def runWithRetry(config: DataPipelineConfig, retries: Int)(implicit spark: SparkSession): Unit = {
    var attempts = 0
    var success = false
    while (attempts < retries && !success) {
      try {
        new DataPipeline(config).run()
        success = true
      } catch {
        case NonFatal(e) =>
          attempts += 1
          logger.error(s"Error on attempt $attempts to execute pipeline ${config.name}", e)
          if (attempts >= retries) {
            logger.error(s"Reached the maximum number of retries for pipeline ${config.name}")
          }
      }
    }
  }
}

