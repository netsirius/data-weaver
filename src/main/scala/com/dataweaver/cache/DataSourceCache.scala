package com.dataweaver.cache

import com.dataweaver.config.DataSourceConfig
import com.dataweaver.factory.DataSourceFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

/** A cache for data sources to ensure data source reuse.
  */
object DataSourceCache {
  // A mutable map to store DataFrames with source IDs as keys
  private val cache = scala.collection.mutable.Map[String, DataFrame]()

  /** Get or create a DataFrame for a given data source configuration.
    *
    * @param sourceConfig
    *   The configuration of the data source.
    * @param spark
    *   The SparkSession to use for DataFrame operations.
    * @return
    *   A DataFrame for the data source.
    */
  def getOrCreate(sourceConfig: DataSourceConfig)(implicit spark: SparkSession): DataFrame = {
    // Check if the DataFrame for the source ID already exists in the cache
    cache.getOrElseUpdate(
      sourceConfig.id, {
        // If not in cache, create the data source and read data
        val source = DataSourceFactory.create(sourceConfig)
        source.readData
      }
    )
  }
}
