package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SourceConnector extends Serializable {
  def connectorType: String
  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame
  def healthCheck(config: Map[String, String]): Either[String, Long] =
    Left(s"Health check not implemented for $connectorType")
}
