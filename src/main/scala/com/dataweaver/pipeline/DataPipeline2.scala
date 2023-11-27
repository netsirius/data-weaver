//
//package com.dataweaver.pipeline
//
//import com.dataweaver.cache.DataSourceCache
//import com.dataweaver.config._
//import com.dataweaver.factory.{DataSinkFactory, TransformationFactory}
//import org.apache.log4j.LogManager
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
///** Class for executing data pipelines with optimized resource management.
// *
// * @param config
// *   Configuration details for the data pipeline.
// */
//class DataPipeline(config: DataPipelineConfig)(implicit spark: SparkSession) {
//
//  private val logger = LogManager.getLogger(getClass)
//
//  /** Executes the data pipeline.
//   */
//  def run(): Unit = {
//    logger.info("Starting data pipeline execution.")
//
//    // Initialize an empty map for storing transformation results.
//    var transformationResults: Map[String, DataFrame] = Map()
//
//    // Sort transformations based on data dependencies.
//    val sortedTransformations = sortTransformations(config.transformations)
//
//    // Iterate over each transformation in sorted order.
//    sortedTransformations.foreach { transConfig =>
//      logger.info(s"Applying transformation: ${transConfig.id}")
//
//      // Load required DataFrames on demand.
//      val requiredDFs = loadRequiredDataFrames(transConfig, transformationResults)
//
//      // Apply the transformation.
//      val transformation = TransformationFactory.create(transConfig)
//      val transformedDF = transformation.applyTransformation(requiredDFs)
//
//      // Update transformation results.
//      transformationResults += (transConfig.id -> transformedDF)
//
//      // Release DataFrames that are no longer needed.
//      releaseUnusedDataFrames(requiredDFs, transformationResults)
//
//      logger.info(s"Transformation '${transConfig.id}' applied successfully.")
//    }
//
//    // Retrieve the final DataFrame for writing to sinks.
//    val finalDF = transformationResults.last._2
//    logger.info("All transformations applied successfully.")
//
//    // Write the final DataFrame to sinks.
//    writeToSinks(finalDF, config.sinks)
//
//    logger.info("Data pipeline execution completed.")
//  }
//
//  /** Sorts transformations based on their data dependencies.
//   *
//   * @param transformations
//   *   A sequence of TransformationConfig objects.
//   * @return
//   *   A sorted sequence of TransformationConfig, where each transformation is ordered after its
//   *   dependencies.
//   */
//  private def sortTransformations(
//                                   transformations: Seq[TransformationConfig]
//                                 ): Seq[TransformationConfig] = {
//    // Construct a dependency map linking each transformation to its dependencies.
//    val dependencyMap = transformations.map(t => t.id -> t.sources).toMap
//
//    /** Checks if all dependencies for a given transformation have been met.
//     *
//     * @param t
//     *   The transformation to check.
//     * @param completed
//     *   A set of IDs of transformations that have been completed.
//     * @return
//     *   True if all dependencies of the transformation are in the completed set.
//     */
//    def dependenciesMet(t: TransformationConfig, completed: Set[String]): Boolean =
//      t.sources.forall(completed.contains)
//
//    var sortedTransformations = Seq.empty[TransformationConfig]
//    var completedTransformations = Set.empty[String]
//    var remainingTransformations = transformations
//
//    // While there are remaining transformations, try to find ones that can be executed.
//    while (remainingTransformations.nonEmpty) {
//      // Partition transformations into those ready to be executed and those not ready.
//      val (ready, notReady) =
//        remainingTransformations.partition(t => dependenciesMet(t, completedTransformations))
//
//      // If no transformations are ready, it indicates a cyclic dependency or missing transformation dependency.
//      if (ready.isEmpty) {
//        throw new IllegalStateException(
//          "Cyclic dependency detected or missing transformation dependency."
//        )
//      }
//
//      // Add ready transformations to the sorted list and mark them as completed.
//      sortedTransformations ++= ready
//      completedTransformations ++= ready.map(_.id)
//      remainingTransformations = notReady
//    }
//
//    sortedTransformations
//  }
//
//  /** Loads required DataFrames for a given transformation.
//   *
//   * @param transConfig
//   *   The transformation configuration.
//   * @param results
//   *   Map of existing transformation results.
//   * @return
//   *   Map of required DataFrames.
//   */
//  private def loadRequiredDataFrames(
//                                      transConfig: TransformationConfig,
//                                      results: Map[String, DataFrame]
//                                    ): Map[String, DataFrame] = {
//    // Load DataFrames necessary for the current transformation.
//    // Only load those that are not already in 'results'.
//    transConfig.sources.map { sourceId =>
//      if (!results.contains(sourceId)) {
//        val sourceConfig = config.dataSources.find(_.id == sourceId).get
//        logger.info(s"Fetching data for source: $sourceId")
//        val df = DataSourceCache.getOrCreate(sourceConfig)
//        (sourceId, df)
//      } else {
//        (sourceId, results(sourceId))
//      }
//    }.toMap
//  }
//
//  /** Releases unused DataFrames from memory.
//   *
//   * @param requiredDFs
//   *   DataFrames required for the current transformation.
//   * @param results
//   *   Map of transformation results.
//   */
//  private def releaseUnusedDataFrames(
//                                       requiredDFs: Map[String, DataFrame],
//                                       results: Map[String, DataFrame]
//                                     ): Unit = {
//    // Release DataFrames that are no longer needed.
//    results.keys.foreach { key =>
//      if (!requiredDFs.contains(key)) {
//        val df = results(key)
//        // Implement logic to release the DataFrame.
//        // For example, if using Spark DataFrames, it could be df.unpersist().
//      }
//    }
//  }
//
//  /** Writes the final DataFrame to the configured sinks.
//   *
//   * @param finalDF
//   *   The final DataFrame to be written.
//   * @param sinks
//   *   Sequence of sink configurations.
//   */
//  private def writeToSinks(finalDF: DataFrame, sinks: Seq[SinkConfig])(implicit
//                                                                       spark: SparkSession
//  ): Unit = {
//    // Write the final DataFrame to each sink.
//    sinks.foreach { sinkConfig =>
//      logger.info(s"Writing data to sink: ${sinkConfig.id}")
//      val sink = DataSinkFactory.create(sinkConfig)
//      sink.writeData(finalDF, config.name)
//      logger.info(s"Data written to sink '${sinkConfig.id}' successfully.")
//    }
//  }
//
//}
