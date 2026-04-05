package com.dataweaver.core.config

import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.Source
import scala.util.Try

object YAMLParser {

  def parseFile(path: String): Either[String, PipelineConfig] = {
    Try {
      val source = Source.fromFile(path)
      try source.mkString
      finally source.close()
    }.toEither.left
      .map(e => s"Cannot read file '$path': ${e.getMessage}")
      .flatMap(parseString)
  }

  def parseString(yaml: String): Either[String, PipelineConfig] = {
    parser
      .parse(yaml)
      .flatMap(_.as[PipelineConfig])
      .left
      .map(e => s"YAML parse error: ${e.getMessage}")
  }
}
