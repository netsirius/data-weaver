package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}

trait TransformPlugin extends Serializable {
  def transformType: String
  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame
}

case class TransformConfig(
    id: String,
    sources: List[String],
    query: Option[String] = None,
    action: Option[String] = None,
    extra: Map[String, String] = Map.empty
)
