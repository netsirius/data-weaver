package com.dataweaver.factory

import com.dataweaver.config.DataSourceConfig
import com.dataweaver.reader.{DataReader, SQLReader, TestReader}

/** Factory for creating data source instances based on the provided configuration.
  */
object DataSourceFactory {

  /** Creates a data source instance based on the provided configuration.
    *
    * @param config
    *   The configuration of the data source.
    * @return
    *   A data source instance.
    * @throws IllegalArgumentException
    *   If the data source type is not supported.
    */
  def create(config: DataSourceConfig): DataReader = {
    config.`type` match {
      case "Test"  => new TestReader(config)
      case "MySQL" => new SQLReader(config)
      case _       => throw new IllegalArgumentException("Data source type is not supported")
    }
  }
}
