package com.dataweaver.config

/**
 * Represents the configuration for a data source.
 *
 * @param id     The unique identifier for the data source.
 * @param `type` The type of the data source.
 * @param config A map of configuration properties for the data source.
 */
case class DataSourceConfig(id: String, `type`: String, config: Map[String, String])

