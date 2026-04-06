package com.dataweaver.core.config

case class PipelineConfig(
    name: String,
    tag: String = "",
    engine: String = "auto",
    dataSources: List[DataSourceConfig] = List.empty,
    transformations: List[TransformationConfig] = List.empty,
    sinks: List[SinkConfig] = List.empty,
    profiles: Option[Map[String, Map[String, String]]] = None,
    tests: List[InlineTestConfig] = List.empty
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
    config: Map[String, String] = Map.empty,
    checks: List[String] = List.empty,
    onFail: String = "abort"
)

case class SinkConfig(
    id: String,
    `type`: String,
    source: Option[String] = None,
    connection: Option[String] = None,
    config: Map[String, String] = Map.empty
)

/** Inline test definition within pipeline YAML. */
case class InlineTestConfig(
    name: String,
    assert: String
)

/** External test definition from .test.yaml files. */
case class ExternalTestConfig(
    name: String,
    transform: String,
    mock_sources: Map[String, List[Map[String, Any]]] = Map.empty,
    expect: TestExpectation = TestExpectation()
)

case class TestExpectation(
    schema: List[Map[String, String]] = List.empty,
    row_count: Option[Int] = None,
    values: List[Map[String, Any]] = List.empty
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
