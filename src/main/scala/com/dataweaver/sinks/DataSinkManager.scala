package com.dataweaver.sinks

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSinkManager {
  def writeData(data: DataFrame, spark: SparkSession): Unit
}
