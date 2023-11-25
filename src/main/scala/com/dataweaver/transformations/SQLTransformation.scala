package com.dataweaver.transformations

import com.dataweaver.config.TransformationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * A Transformation implementation for applying SQL transformations to DataFrames.
 *
 * @param config The configuration for the SQL transformation.
 */
class SQLTransformation(config: TransformationConfig) extends Transformation {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Applies the SQL transformation to the provided DataFrames using temporary views.
   *
   * @param dataFrames A map of source DataFrame names to DataFrames.
   * @param spark      The SparkSession for executing the transformation.
   * @return A DataFrame containing the result of the SQL transformation.
   */
  override def applyTransformation(dataFrames: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {

    // Ensure that the query is defined
    val query = config.query.getOrElse(throw new IllegalArgumentException("Query is required"))

    // Ensure that all required sources are available
    config.sources.foreach { sourceId =>
      if (!dataFrames.contains(sourceId)) {
        throw new IllegalArgumentException(s"Source not found: $sourceId")
      }
    }

    // Register each DataFrame as a temporary view if not already done
    config.sources.foreach { sourceId =>
      dataFrames(sourceId).createOrReplaceTempView(sourceId)
    }

    // Execute the SQL query using the temporary views
    try {
      val df = spark.sql(query)
      // Ensure query is correct before executing
      df.queryExecution.analyzed
      df
    } catch {
      case e: Exception =>
        logger.error(s"Error executing SQL query: $query", e)
        throw e
    }
  }
}