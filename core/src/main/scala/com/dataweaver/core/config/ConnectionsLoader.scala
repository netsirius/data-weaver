package com.dataweaver.core.config

import io.circe.yaml.parser
import org.apache.log4j.LogManager
import scala.io.Source
import scala.util.Try

object ConnectionsLoader {
  private val logger = LogManager.getLogger(getClass)
  type ConnectionMap = Map[String, Map[String, String]]

  def load(path: String): ConnectionMap = {
    Try {
      val source = Source.fromFile(path)
      val content = try source.mkString finally source.close()
      parser.parse(content) match {
        case Right(json) =>
          val connections = json.hcursor.downField("connections")
          connections.keys.map(_.toList).getOrElse(Nil).flatMap { name =>
            connections.downField(name).as[Map[String, String]].toOption.map(name -> _)
          }.toMap
        case Left(err) =>
          logger.error(s"Failed to parse connections file '$path': ${err.getMessage}")
          Map.empty
      }
    }.getOrElse {
      logger.warn(s"Connections file not found: $path")
      Map.empty[String, Map[String, String]]
    }
  }

  def resolveConnection(name: String, connections: ConnectionMap): Map[String, String] = {
    connections.getOrElse(name,
      throw new IllegalArgumentException(
        s"Connection '$name' not found. Available: ${connections.keys.mkString(", ")}"))
  }
}
