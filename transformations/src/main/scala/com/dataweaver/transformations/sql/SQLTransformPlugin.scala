package com.dataweaver.transformations.sql

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class SQLTransformPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)

  def transformType: String = "SQL"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val query = config.query.getOrElse(
      throw new IllegalArgumentException(s"Transform '${config.id}' requires a 'query' field"))

    config.sources.foreach { sourceId =>
      inputs.getOrElse(sourceId,
        throw new IllegalArgumentException(s"Source '$sourceId' not found for transform '${config.id}'"))
        .createOrReplaceTempView(sourceId)
    }

    try {
      val df = spark.sql(query)
      df.queryExecution.analyzed
      df
    } catch {
      case e: Exception =>
        logger.error(s"SQL error in transform '${config.id}': ${e.getMessage}")
        throw e
    }
  }
}
