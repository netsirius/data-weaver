package com.dataweaver.core

import com.dataweaver.config.{DataPipelineConfig, DataWeaverConfig}
import com.dataweaver.parsing.YAMLParser
import com.dataweaver.pipeline.DataPipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success}

/**
 * Manages the execution of data flows defined in YAML configuration files.
 */
class DataFlowManager {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes data flows based on the specified tag and regex criteria.
   *
   * @param tag   Optional tag for filtering data flows.
   * @param regex Optional regular expression for filtering YAML files.
   */
  def executeDataFlows(tag: Option[String], regex: Option[String]): Unit = {
    implicit val spark = SparkSession.builder.appName("DataWeaverApp").getOrCreate()
    try {
      val yamlFiles = getYamlFiles(tag, regex)

      yamlFiles.foreach { path =>
        val config = YAMLParser.parse(path.toFile.getAbsolutePath)
        config.foreach { pipelineConfig =>
          if (tag.forall(pipelineConfig.tag.contains)) {
            runPipeline(pipelineConfig)
          }
        }
      }
    } finally {
      spark.stop()
    }
  }

  /**
   * Retrieves the YAML files to be processed, filtered by the provided tag and regex.
   *
   * @param tag   Optional tag for filtering.
   * @param regex Optional regex for filtering.
   * @return A sequence of Paths to the YAML files.
   */
  private def getYamlFiles(tag: Option[String], regex: Option[String]): Seq[java.nio.file.Path] = {
    DataWeaverConfig.load() match {
      case Success(config) =>
        logger.info("Project config loaded successfully.")
        val yamlDirectory = config.getPipelinesDir
        Files.list(Paths.get(yamlDirectory))
          .toArray
          .map(_.asInstanceOf[java.nio.file.Path])
          .filter(path => regex.forall(r => path.toString.matches(r)) || tag.forall(t => path.toString.contains(t)))
      case Failure(exception) =>
        logger.error(s"Failed to load project configuration: ${exception.getMessage}")
        Seq.empty
    }
  }

  /**
   * Runs a single data pipeline defined in a DataPipelineConfig.
   *
   * @param pipelineConfig The configuration for the data pipeline.
   */
  private def runPipeline(pipelineConfig: DataPipelineConfig)(implicit spark: SparkSession): Unit = {
    try {
      new DataPipeline(pipelineConfig).run()
    } catch {
      case e: Exception =>
        logger.error(s"Error running pipeline ${pipelineConfig.name}: ${e.getMessage}", e)
    }
  }
}
