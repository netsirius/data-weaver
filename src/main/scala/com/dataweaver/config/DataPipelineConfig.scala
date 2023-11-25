package com.dataweaver.config


/**
 * Represents the configuration for a data pipeline.
 *
 * @param name            The name of the data pipeline.
 * @param tag             The tag associated with the data pipeline.
 * @param dataSources     The list of data source configurations.
 * @param transformations The list of transformation configurations.
 * @param sinks           The list of sink configurations.
 */
case class DataPipelineConfig(
                               name: String,
                               tag: String,
                               dataSources: List[DataSourceConfig],
                               transformations: List[TransformationConfig],
                               sinks: List[SinkConfig]
                             )

/**
 * Represents the configuration for a data source.
 *
 * @param id     The unique identifier for the data source.
 * @param `type` The type of the data source.
 * @param query  The query to be executed for the data source.
 * @param config A map of configuration properties for the data source.
 */
case class DataSourceConfig(
                             id: String,
                             `type`: String,
                             query: String, // Añadido basado en el nuevo esquema
                             config: Map[String, String]
                           )

/**
 * Represents the configuration for a data transformation.
 *
 * @param id
 * @param `type`
 * @param sources
 * @param query
 * @param action
 */
case class TransformationConfig(
                                 id: String,
                                 `type`: String,
                                 sources: List[String],
                                 query: Option[String],
                                 action: Option[String] // Modificado para ser opcional, basado en el nuevo esquema
                               )

/**
 * Represents the configuration for a data sink.
 *
 * @param id
 * @param `type`
 * @param config
 */
case class SinkConfig(
                       id: String,
                       `type`: String,
                       config: Map[String, String] // Incluye saveMode y otros parámetros de configuración
                     )

/**
 * Represents the execution mode for a data pipeline.
 *
 * @param Read The data pipeline will read data from the data sources.
 */
object ExecutionMode extends Enumeration {
  type ExecutionMode = Value

  val Test, Debug, Production = Value
}