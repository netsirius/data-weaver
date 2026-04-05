package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestSourceConnector extends SourceConnector {
  def connectorType: String = "Test"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val basePath = sys.props.getOrElse("dataweaver.test.resourcesDir", "core/src/test/resources")
    val id = config.getOrElse("id",
      throw new IllegalArgumentException("TestSourceConnector requires 'id' in config"))
    val filePath = s"$basePath/input_files/$id.json"
    spark.read.option("multiLine", true).json(filePath)
  }
}
