package com.dataweaver.runners

import com.dataweaver.core.DataFlowManager
import org.apache.spark.sql.SparkSession

/**
 * A runner for executing Spark jobs locally.
 */
class LocalSparkRunner extends Runner {
  /**
   * Run the Spark job locally using the specified pipelines and configurations.
   *
   * @param pipelinesFolder The folder containing pipeline files.
   * @param tag             An optional tag to filter pipeline files.
   * @param regex           An optional regex pattern to filter pipeline files.
   */
  override def run(pipelinesFolder: String, tag: Option[String], regex: Option[String]): Unit = {
    // Create a local Spark session
    implicit val spark: SparkSession = SparkSession.builder
      .master("local[4]") // You can adjust the local configuration as needed
      .appName("DataWeaverLocal")
      .getOrCreate()

    try {
      val manager = new DataFlowManager()
      manager.executeDataFlows(tag, regex)
    } finally {
      // Stop the Spark session when done
      spark.stop()
    }
  }
}

