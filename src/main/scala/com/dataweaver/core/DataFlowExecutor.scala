package com.dataweaver.core

import com.dataweaver.config.ExecutionMode._
import com.dataweaver.config.{DataPipelineConfig, DataWeaverConfig}
import com.dataweaver.parsing.YAMLParser
import com.dataweaver.pipeline.DataPipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
 * Manages the execution of data flows defined in YAML configuration files.
 */
class DataFlowExecutor(appConfig: DataWeaverConfig) {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes data flows based on the specified tag and regex criteria.
   *
   * @param tag   Optional tag for filtering data flows.
   * @param regex Optional regular expression for filtering YAML files.
   */
  def executeDataFlows(tag: Option[String], regex: Option[String], executionMode: ExecutionMode = Production)
                      (implicit spark: SparkSession): Unit = {
    Try {
      val pipelineConfigs = getYamlFiles(tag, regex)
        .flatMap(path => YAMLParser.parse(path.toFile.getAbsolutePath))

      val updatedPipelineConfigs = transformConfigs(pipelineConfigs, executionMode)

      // Filter pipelines by tag and regex
      val filteredPipelineConfigs = updatedPipelineConfigs.filter { config =>
        regex.forall(r => config.name.matches(r)) || tag.forall(t => config.tag.contains(t))
      }

      // TODO: Parallelize execution of data flows
      filteredPipelineConfigs.foreach { config =>
        runPipeline(config)
      }

    }.recover {
      case e: Exception =>
        logger.error(s"Error executing data flows: ${e.getMessage}", e)
        throw e
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
    val yamlDirectory = appConfig.getPipelinesDir
    Files.list(Paths.get(yamlDirectory))
      .toArray
      .map(_.asInstanceOf[java.nio.file.Path])
      .filter(path => regex.forall(r => path.toString.matches(r)) || tag.forall(t => path.toString.contains(t)))
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

  /**
   * Updates the types of data sources and sinks to "Test" for testing purposes.
   *
   * @param pipelineConfig The configuration for the data pipeline.
   * @return The updated configuration.
   */
  private def updateConfigTypes(configs: Seq[DataPipelineConfig]): Seq[DataPipelineConfig] = {
    configs.map { pipelineConfig =>
      val updatedDataSources = pipelineConfig.dataSources.map(dataSource => dataSource.copy(`type` = "Test"))
      val updatedSinks = pipelineConfig.sinks.map(sink => sink.copy(`type` = "Test"))
      pipelineConfig.copy(dataSources = updatedDataSources, sinks = updatedSinks)
    }
  }

  /**
   * Transforms the types of data sources and sinks based on the execution mode.
   *
   * @param configs
   * @param mode
   * @return
   */
  def transformConfigs(configs: Seq[DataPipelineConfig], mode: ExecutionMode): Seq[DataPipelineConfig] = {
    mode match {
      case Test => updateConfigTypes(configs)
      case _ => configs
    }
  }
}
