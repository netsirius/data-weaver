package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestSinkConnector extends SinkConnector {
  def connectorType: String = "Test"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val basePath = sys.props.getOrElse("dataweaver.test.resourcesDir", "core/src/test/resources")
    val outputPath = s"$basePath/output_test/$pipelineName.json"
    data.coalesce(1).write.mode("overwrite").json(outputPath)
  }
}
