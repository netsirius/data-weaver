package com.dataweaver.sources

import com.dataweaver.utils.Utils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import java.net.URI

class FileSource(filePath: String, config: Map[String, String] = Map()) extends DataSource {

  def path: String = Utils.getConfigValue(config, "file_path")

  def format: String = Utils.getConfigValue(config, "file_format")

  override def readData()(implicit spark: SparkSession): DataFrame = {

    val reader = spark.read.format(format)
    format.toLowerCase match {
      case "csv" => readCsv(URI.create(path), reader)
      // Add cases for other formats here
      case _ => throw new IllegalArgumentException(s"Unsupported file format: $format")
    }
  }

  private def readCsv(fileURI: URI, reader: DataFrameReader): DataFrame = {
    fileURI.getScheme match {
      case "file" => reader.load(fileURI.getPath)
      // Add cases for other URI schemes here
      case _ => throw new IllegalArgumentException(s"Unsupported URI scheme: ${fileURI.getScheme}")
    }
  }
}