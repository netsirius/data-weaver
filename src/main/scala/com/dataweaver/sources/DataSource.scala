package com.dataweaver.sources

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A trait defining the contract for reading data from external data sources.
 */
trait DataSource {
  /**
   * The name of the data source.
   *
   * @return The name of the data source.
   */
  def name: String

  /**
   * Reads data from the data source and returns it as a DataFrame.
   *
   * @param spark The SparkSession for executing the read operation.
   * @return A DataFrame containing the data from the data source.
   */
  def readData()(implicit spark: SparkSession): DataFrame
}

