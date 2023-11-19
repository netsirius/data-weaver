package com.dataweaver.sinks

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A trait defining the contract for writing data to external sinks.
 */
trait DataSink {
  /**
   * Writes the provided DataFrame to an external sink.
   *
   * @param data  The DataFrame to be written to the sink.
   * @param spark The SparkSession for executing the write operation.
   */
  def writeData(data: DataFrame)(implicit spark: SparkSession): Unit
}
