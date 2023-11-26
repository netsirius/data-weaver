//package com.dataweaver.pipeline
//
//import com.dataweaver.cache.DataSourceCache
//import com.dataweaver.config._
//import com.dataweaver.factory.{DataSinkFactory, TransformationFactory}
//import org.apache.log4j.LogManager
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
///** Represents a data pipeline that executes a series of transformations on input data and writes
//  * the result to data sinks.
//  *
//  * @param config
//  *   The configuration of the data pipeline.
//  * @param spark
//  *   The SparkSession used for processing.
//  */
//class DataPipeline2(config: DataPipelineConfig)(implicit spark: SparkSession) {
//
//  private val logger = LogManager.getLogger(getClass)
//
//  /** Executes the data pipeline. This method orchestrates the flow of data through various stages
//    * of the pipeline, including fetching data from sources, applying transformations, and writing
//    * the transformed data to sinks.
//    */
//  def run(): Unit = {
//    logger.info("Starting data pipeline execution.")
//
//    // Initialize a map to hold DataFrames fetched from data sources.
//    val dataFrames: Map[String, DataFrame] = config.dataSources.map { sourceConfig =>
//      logger.info(s"Fetching data for source: ${sourceConfig.id}")
//      val df = DataSourceCache.getOrCreate(sourceConfig)
//      (sourceConfig.id, df)
//    }.toMap
//
//    logger.info("Data sources fetched successfully.")
//
//    // Initialize a map to store the results of transformations.
//    var transformationResults: Map[String, DataFrame] = dataFrames
//
//    // Iterate over each transformation configuration in the pipeline.
//    config.transformations.foreach { transConfig =>
//      logger.info(s"Applying transformation: ${transConfig.id}")
//
//      val transformation = TransformationFactory.create(transConfig)
//      val transformedDF = transformation.applyTransformation(transformationResults)
//
//      transformationResults += (transConfig.id -> transformedDF)
//
//      logger.info(s"Transformation '${transConfig.id}' applied successfully.")
//    }
//
//    // Retrieve the final DataFrame to be written to the sinks.
//    val finalDF = transformationResults.last._2
//    logger.info("All transformations applied successfully.")
//
//    // Iterate over each sink configuration and write the final DataFrame.
//    config.sinks.foreach { sinkConfig =>
//      logger.info(s"Writing data to sink: ${sinkConfig.id}")
//      val sink = DataSinkFactory.create(sinkConfig)
//      sink.writeData(finalDF, config.name)
//      logger.info(s"Data written to sink '${sinkConfig.id}' successfully.")
//    }
//
//    logger.info("Data pipeline execution completed.")
//  }
//
//}
