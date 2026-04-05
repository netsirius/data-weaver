package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileSourceConnector extends SourceConnector {
  def connectorType: String = "File"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val path = config.getOrElse("path",
      throw new IllegalArgumentException("File: 'path' is required"))
    val format = config.getOrElse("format", inferFormat(path))

    val reader = spark.read

    config.filterNot { case (k, _) => Set("path", "format", "id").contains(k) }
      .foreach { case (k, v) => reader.option(k, v) }

    format.toLowerCase match {
      case "csv"     => reader.option("header", config.getOrElse("header", "true")).csv(path)
      case "json"    => reader.option("multiLine", config.getOrElse("multiLine", "true")).json(path)
      case "parquet" => reader.parquet(path)
      case "orc"     => reader.orc(path)
      case other     => throw new IllegalArgumentException(
        s"Unsupported file format '$other'. Supported: csv, json, parquet, orc")
    }
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val path = config.getOrElse("path", return Left("path not configured"))
    val start = System.currentTimeMillis()
    val file = new java.io.File(path)
    if (file.exists() || path.startsWith("s3://") || path.startsWith("gs://"))
      Right(System.currentTimeMillis() - start)
    else
      Left(s"File not found: $path")
  }

  private def inferFormat(path: String): String = {
    val lower = path.toLowerCase
    if (lower.endsWith(".csv")) "csv"
    else if (lower.endsWith(".json") || lower.endsWith(".jsonl")) "json"
    else if (lower.endsWith(".parquet")) "parquet"
    else if (lower.endsWith(".orc")) "orc"
    else "parquet"
  }
}
