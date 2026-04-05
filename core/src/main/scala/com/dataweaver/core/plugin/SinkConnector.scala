package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SinkConnector extends Serializable {
  def connectorType: String
  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit
  def healthCheck(config: Map[String, String]): Either[String, Long] =
    Left(s"Health check not implemented for $connectorType")
}
