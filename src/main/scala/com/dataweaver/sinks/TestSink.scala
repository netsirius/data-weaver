package com.dataweaver.sinks

import org.apache.spark.sql.{DataFrame, SparkSession}

class TestSink extends DataSink {
  /**
   * Writes the provided DataFrame to an external sink.
   *
   * @param data         The DataFrame to be written to the sink.
   * @param pipelineName The name of the pipeline.
   * @param spark        The SparkSession for executing the write operation.
   */
  override def writeData(data: DataFrame, pipelineName: String)(implicit spark: SparkSession): Unit = {
    val outputPath = s"src/test/resources/output_test/$pipelineName.json"

    data.coalesce(1).write.mode("overwrite").json(outputPath)

  }
}
