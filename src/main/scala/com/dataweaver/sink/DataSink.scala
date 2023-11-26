package com.dataweaver.sink

import org.apache.spark.sql.{DataFrame, SparkSession}

/** A trait defining the contract for writing data to external sinks.
  */
trait DataSink extends Serializable {

  /** Writes the provided DataFrame to an external sink.
    *
    * @param data
    *   The DataFrame to be written to the sink.
    * @param pipelineName
    *   The name of the pipeline.
    * @param spark
    *   The SparkSession for executing the write operation.
    */
  def writeData(data: DataFrame, pipelineName: String)(implicit spark: SparkSession): Unit
}
