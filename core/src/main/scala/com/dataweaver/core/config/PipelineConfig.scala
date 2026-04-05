package com.dataweaver.core.config

case class PipelineConfig(
    name: String,
    tag: String = "",
    engine: String = "auto",
    dataSources: List[DataSourceConfig] = List.empty,
    transformations: List[TransformationConfig] = List.empty,
    sinks: List[SinkConfig] = List.empty,
    profiles: Option[Map[String, Map[String, String]]] = None
)

case class DataSourceConfig(
    id: String,
    `type`: String,
    connection: Option[String] = None,
    query: String = "",
    config: Map[String, String] = Map.empty
)

case class TransformationConfig(
    id: String,
    `type`: String,
    sources: List[String] = List.empty,
    query: Option[String] = None,
    action: Option[String] = None,
    config: Map[String, String] = Map.empty
)

case class SinkConfig(
    id: String,
    `type`: String,
    source: Option[String] = None,
    connection: Option[String] = None,
    config: Map[String, String] = Map.empty
)

sealed trait ExecutionMode
object ExecutionMode {
  case object Test extends ExecutionMode
  case object Debug extends ExecutionMode
  case object Production extends ExecutionMode

  def fromString(s: String): ExecutionMode = s.toLowerCase match {
    case "test"       => Test
    case "debug"      => Debug
    case "production" => Production
    case other        => throw new IllegalArgumentException(s"Invalid execution mode: $other")
  }
}
