package com.dataweaver.pipeline

import com.dataweaver.cache.DataSourceCache
import com.dataweaver.config._
import com.dataweaver.factories.{DataSinkFactory, TransformationFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Represents a data pipeline that executes a series of transformations on input data and writes
 * the result to data sinks.
 *
 * @param config The configuration of the data pipeline.
 * @param spark  The SparkSession used for processing.
 */
class DataPipeline(config: DataPipelineConfig)(implicit spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Runs the data pipeline, which includes applying transformations to input data and writing
   * the result to data sinks.
   */
  def run(): Unit = {
    // Obtain and cache DataFrames from data sources, registering them as temporary views.
    val dataFrames = config.dataSources.map { sourceConfig =>
      val df = DataSourceCache.getOrCreate(sourceConfig)
      (sourceConfig.id, df)
    }.toMap

    // Apply each transformation, which may use one or more of the created temporary views.
    var finalDF: DataFrame = null
    config.transformations.foreach { transConfig =>
      val transformation = TransformationFactory.create(transConfig)
      finalDF = transformation.applyTransformation(dataFrames)
    }

    // Write the final DataFrame to data sinks.
    config.sinks.foreach { sinkConfig =>
      val sink = DataSinkFactory.create(sinkConfig)
      sink.writeData(finalDF)
    }
  }
}