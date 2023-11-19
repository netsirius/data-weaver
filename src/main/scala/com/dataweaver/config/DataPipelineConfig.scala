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