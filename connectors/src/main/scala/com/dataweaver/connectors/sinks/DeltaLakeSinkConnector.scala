package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class DeltaLakeSinkConnector extends SinkConnector {
  def connectorType: String = "DeltaLake"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val path = config.getOrElse("path",
      throw new IllegalArgumentException("DeltaLake: 'path' is required"))
    val saveMode = config.getOrElse("saveMode", "Overwrite").toLowerCase
    val partitionBy = config.get("partitionBy").map(_.split(",").map(_.trim).toSeq)

    saveMode match {
      case "merge" =>
        val mergeKey = config.getOrElse("mergeKey",
          throw new IllegalArgumentException("DeltaLake merge requires 'mergeKey'"))
        writeMerge(data, path, mergeKey)

      case mode =>
        var writer = data.write
          .format("delta")
          .mode(SaveMode.valueOf(mode.capitalize))
        partitionBy.foreach(cols => writer = writer.partitionBy(cols: _*))
        writer.save(path)
    }
  }

  private def writeMerge(data: DataFrame, path: String, mergeKey: String)(implicit
      spark: SparkSession
  ): Unit = {
    import io.delta.tables.DeltaTable

    if (DeltaTable.isDeltaTable(spark, path)) {
      val deltaTable = DeltaTable.forPath(spark, path)
      val mergeKeys = mergeKey.split(",").map(_.trim)
      val condition = mergeKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

      deltaTable.as("target")
        .merge(data.as("source"), condition)
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    } else {
      data.write.format("delta").save(path)
    }
  }
}
