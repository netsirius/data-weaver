package com.dataweaver.core.config

import io.circe.{Decoder, HCursor}
import io.circe.yaml.parser

import scala.io.Source
import scala.util.Try

object YAMLParser {

  // Custom decoders that handle missing optional fields with defaults
  implicit val dataSourceConfigDecoder: Decoder[DataSourceConfig] = (c: HCursor) =>
    for {
      id <- c.get[String]("id")
      tpe <- c.get[String]("type")
      connection <- c.get[Option[String]]("connection")
      query <- c.getOrElse[String]("query")("")
      config <- c.getOrElse[Map[String, String]]("config")(Map.empty)
    } yield DataSourceConfig(id, tpe, connection, query, config)

  implicit val transformationConfigDecoder: Decoder[TransformationConfig] = (c: HCursor) =>
    for {
      id <- c.get[String]("id")
      tpe <- c.get[String]("type")
      sources <- c.getOrElse[List[String]]("sources")(List.empty)
      query <- c.get[Option[String]]("query")
      action <- c.get[Option[String]]("action")
      config <- c.getOrElse[Map[String, String]]("config")(Map.empty)
    } yield TransformationConfig(id, tpe, sources, query, action, config)

  implicit val sinkConfigDecoder: Decoder[SinkConfig] = (c: HCursor) =>
    for {
      id <- c.get[String]("id")
      tpe <- c.get[String]("type")
      source <- c.get[Option[String]]("source")
      connection <- c.get[Option[String]]("connection")
      config <- c.getOrElse[Map[String, String]]("config")(Map.empty)
    } yield SinkConfig(id, tpe, source, connection, config)

  implicit val pipelineConfigDecoder: Decoder[PipelineConfig] = (c: HCursor) =>
    for {
      name <- c.get[String]("name")
      tag <- c.getOrElse[String]("tag")("")
      engine <- c.getOrElse[String]("engine")("auto")
      dataSources <- c.getOrElse[List[DataSourceConfig]]("dataSources")(List.empty)
      transformations <- c.getOrElse[List[TransformationConfig]]("transformations")(List.empty)
      sinks <- c.getOrElse[List[SinkConfig]]("sinks")(List.empty)
      profiles <- c.get[Option[Map[String, Map[String, String]]]]("profiles")
    } yield PipelineConfig(name, tag, engine, dataSources, transformations, sinks, profiles)

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
