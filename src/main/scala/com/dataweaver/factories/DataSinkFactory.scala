package com.dataweaver.factories

import com.dataweaver.config.SinkConfig
import com.dataweaver.sinks.{BigQuerySink, DataSink, TestSink}

/**
 * Factory for creating data sink instances based on the provided configuration.
 */
object DataSinkFactory {

  /**
   * Creates a data sink instance based on the provided configuration.
   *
   * @param config The configuration of the data sink.
   * @return A data sink instance.
   * @throws IllegalArgumentException If the data sink type is not supported.
   */
  def create(config: SinkConfig): DataSink = {
    config.`type` match {
      case "Test" => new TestSink
      case "BigQuery" => new BigQuerySink(config)
      case _ => throw new IllegalArgumentException("Data sink type is not supported")
    }
  }
}