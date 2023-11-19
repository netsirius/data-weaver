package com.dataweaver.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A trait defining the contract for applying transformations to DataFrames.
 */
trait Transformation {
  /**
   * Applies a transformation to the provided DataFrames and returns the result as a DataFrame.
   *
   * @param dataFrames A map of source DataFrame names to DataFrames.
   * @param spark      The SparkSession for executing the transformation.
   * @return A DataFrame containing the result of the transformation.
   */
  def applyTransformation(dataFrames: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame
}
