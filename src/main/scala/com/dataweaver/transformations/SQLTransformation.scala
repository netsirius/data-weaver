package com.dataweaver.transformations

import com.dataweaver.config.TransformationConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A Transformation implementation for applying SQL transformations to DataFrames.
 *
 * @param config The configuration for the SQL transformation.
 */
class SQLTransformation(config: TransformationConfig) extends Transformation {

  /**
   * Applies the SQL transformation to the provided DataFrames using temporary views.
   *
   * @param dataFrames A map of source DataFrame names to DataFrames.
   * @param spark      The SparkSession for executing the transformation.
   * @return A DataFrame containing the result of the SQL transformation.
   */
  override def applyTransformation(dataFrames: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
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
    spark.sql(config.query)
  }
}
