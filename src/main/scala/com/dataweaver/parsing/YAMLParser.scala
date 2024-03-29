package com.dataweaver.parsing

import com.dataweaver.config._
import io.circe.generic.auto._
import io.circe.yaml.parser._
import org.apache.log4j.LogManager

/** Utility for parsing YAML configuration files into a list of DataPipelineConfig objects.
  */
object YAMLParser {
  private val logger = LogManager.getLogger(getClass)

  /** Parses a YAML configuration file and returns a list of DataPipelineConfig objects.
    *
    * @param pipelineFilePath
    *   The path to the YAML configuration file.
    * @return
    *   A list of DataPipelineConfig objects.
    */
  def parse(pipelineFilePath: String): Option[DataPipelineConfig] = {
    val source = scala.io.Source.fromFile(pipelineFilePath)
    val content = source.mkString
    source.close()

    logger.info(s"Parsing YAML file: $pipelineFilePath")
    logger.debug(s"YAML file contents: $content")

    decode[DataPipelineConfig](content) match {
      case Right(config) => Some(config)
      case Left(error) =>
        logger.error("Error parsing the YAML file", error)
        None
    }
  }
}
