package com.dataweaver.sinks

import com.dataweaver.config.SinkConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Implementation of a BigQuery sink for writing data to BigQuery tables.
 *
 * @param config The configuration for the BigQuery sink.
 */
class BigQuerySink(config: SinkConfig) extends DataSink {

  /**
   * Writes the provided DataFrame to a BigQuery table.
   *
   * @param data  The DataFrame to be written to BigQuery.
   * @param spark The SparkSession for executing the write operation.
   * @throws IllegalArgumentException If required configuration values are missing.
   */
  override def writeData(data: DataFrame)(implicit spark: SparkSession): Unit = {
    val projectId = config.config.getOrElse("projectId", throw new IllegalArgumentException("Project ID is required"))
    val datasetName = config.config.getOrElse("datasetName", throw new IllegalArgumentException("Dataset name is required"))
    val tableName = config.config.getOrElse("tableName", throw new IllegalArgumentException("Table name is required"))
    val temporaryGcsBucket = config.config.getOrElse("temporaryGcsBucket", throw new IllegalArgumentException("GCS Bucket is required"))

    val bqTable = s"$projectId:$datasetName.$tableName"

    // Specify other options as needed, such as authentication key
    val options = Map(
      "table" -> bqTable,
      "temporaryGcsBucket" -> "<TEMP_BUCKET>" // Replace with the name of your temporary GCS bucket
    )

    // Write the DataFrame to BigQuery
    data.write
      .format("bigquery")
      .options(options)
      .mode(SaveMode.Append) // Or choose another save mode according to your needs
      .save()
  }
}
