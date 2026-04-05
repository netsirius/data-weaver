package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class FileSinkConnector extends SinkConnector {
  def connectorType: String = "File"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val path = config.getOrElse("path",
      throw new IllegalArgumentException("File sink: 'path' is required"))
    val format = config.getOrElse("format", "parquet")
    val saveMode = SaveMode.valueOf(config.getOrElse("saveMode", "Overwrite"))
    val partitionBy = config.get("partitionBy").map(_.split(",").map(_.trim).toSeq)
    val coalesce = config.get("coalesce").map(_.toInt)

    val df = coalesce.map(n => data.coalesce(n)).getOrElse(data)
    var writer = df.write.mode(saveMode)
    partitionBy.foreach(cols => writer = writer.partitionBy(cols: _*))

    format.toLowerCase match {
      case "csv"     => writer.option("header", "true").csv(path)
      case "json"    => writer.json(path)
      case "parquet" => writer.parquet(path)
      case "orc"     => writer.orc(path)
      case other     => throw new IllegalArgumentException(
        s"Unsupported file format '$other'. Supported: csv, json, parquet, orc")
    }
  }
}
