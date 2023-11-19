package com.dataweaver.config

/**
 * Represents the configuration for a data sink.
 *
 * @param id     The unique identifier for the data sink.
 * @param `type` The type of the data sink.
 * @param config A map of configuration properties for the data sink.
 */
case class SinkConfig(id: String, `type`: String, config: Map[String, String])

