package com.dataweaver.parsing

import com.dataweaver.config._
import io.circe.generic.auto._
import io.circe.parser._
import org.slf4j.LoggerFactory

/**
 * Utility for parsing YAML configuration files into a list of DataPipelineConfig objects.
 */
object YAMLParser {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Parses a YAML configuration file and returns a list of DataPipelineConfig objects.
   *
   * @param configFilePath The path to the YAML configuration file.
   * @return A list of DataPipelineConfig objects.
   */
  def parse(configFilePath: String): List[DataPipelineConfig] = {
    val source = scala.io.Source.fromFile(configFilePath)
    val content = source.mkString
    source.close()

    decode[List[DataPipelineConfig]](content) match {
      case Right(configs) => configs
      case Left(error) =>
        logger.error("Error parsing the YAML file", error)
        List.empty
    }
  }
}