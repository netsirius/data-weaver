package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class BigQuerySinkConnector extends SinkConnector {
  def connectorType: String = "BigQuery"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val projectId = config.getOrElse("projectId", throw new IllegalArgumentException("projectId is required"))
    val datasetName = config.getOrElse("datasetName", throw new IllegalArgumentException("datasetName is required"))
    val tableName = config.getOrElse("tableName", throw new IllegalArgumentException("tableName is required"))
    val temporaryGcsBucket = config.getOrElse("temporaryGcsBucket", throw new IllegalArgumentException("temporaryGcsBucket is required"))
    val saveMode = config.getOrElse("saveMode", throw new IllegalArgumentException("saveMode is required"))

    val bqTable = s"$projectId:$datasetName.$tableName"

    data.write
      .format("bigquery")
      .option("table", bqTable)
      .option("temporaryGcsBucket", temporaryGcsBucket)
      .mode(SaveMode.valueOf(saveMode))
      .save()
  }
}
