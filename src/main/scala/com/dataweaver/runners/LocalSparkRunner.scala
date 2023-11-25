package com.dataweaver.runners

import com.dataweaver.config.DataWeaverConfig
import com.dataweaver.core.DataFlowExecutor
import org.apache.spark.sql.SparkSession

/** A runner for executing Spark jobs locally.
 */
class LocalSparkRunner extends Runner {

  /**
   * Run the Spark job using the specified pipelines and configurations.
   *
   * @param projectConfig The project config
   * @param tag           An optional tag to filter pipeline files.
   * @param regex         An optional regex pattern to filter pipeline files.
   */
  override def run(
                    projectConfig: DataWeaverConfig,
                    tag: Option[String],
                    regex: Option[String]
                  ): Unit = {
    // Create a local Spark session
    implicit val spark: SparkSession = SparkSession.builder
      .appName("DataWeaverLocal")
      .getOrCreate()

    try {
      val manager = new DataFlowExecutor(projectConfig)
      manager.executeDataFlows(tag, regex)
    } finally {
      // Stop the Spark session when done
      spark.stop()
    }
  }
}
