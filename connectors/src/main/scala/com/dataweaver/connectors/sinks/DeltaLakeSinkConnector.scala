package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Writes data to Delta Lake tables.
  * Supports Overwrite, Append modes via Spark's delta format.
  * Merge mode requires delta-spark library on the classpath.
  *
  * Note: delta-spark is an optional runtime dependency.
  * For merge, add io.delta:delta-spark to your Spark classpath.
  */
class DeltaLakeSinkConnector extends SinkConnector {
  private val logger = LogManager.getLogger(getClass)

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

  /** Merge using Delta Lake API via reflection (avoids compile-time dependency). */
  private def writeMerge(data: DataFrame, path: String, mergeKey: String)(implicit
      spark: SparkSession
  ): Unit = {
    try {
      val deltaTableClass = Class.forName("io.delta.tables.DeltaTable")
      val isDeltaMethod = deltaTableClass.getMethod("isDeltaTable", classOf[SparkSession], classOf[String])
      val isDelta = isDeltaMethod.invoke(null, spark, path).asInstanceOf[Boolean]

      if (isDelta) {
        val forPathMethod = deltaTableClass.getMethod("forPath", classOf[SparkSession], classOf[String])
        val deltaTable = forPathMethod.invoke(null, spark, path)

        val mergeKeys = mergeKey.split(",").map(_.trim)
        val condition = mergeKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

        val asMethod = deltaTableClass.getMethod("as", classOf[String])
        val targetAlias = asMethod.invoke(deltaTable, "target")

        val mergeMethod = deltaTableClass.getMethod("merge", classOf[DataFrame], classOf[String])
        val mergeBuilder = mergeMethod.invoke(targetAlias, data.as("source"), condition)

        val whenMatchedMethod = mergeBuilder.getClass.getMethod("whenMatched")
        val matchedBuilder = whenMatchedMethod.invoke(mergeBuilder)

        val updateAllMethod = matchedBuilder.getClass.getMethod("updateAll")
        val mergeBuilder2 = updateAllMethod.invoke(matchedBuilder)

        val whenNotMatchedMethod = mergeBuilder2.getClass.getMethod("whenNotMatched")
        val notMatchedBuilder = whenNotMatchedMethod.invoke(mergeBuilder2)

        val insertAllMethod = notMatchedBuilder.getClass.getMethod("insertAll")
        val finalBuilder = insertAllMethod.invoke(notMatchedBuilder)

        val executeMethod = finalBuilder.getClass.getMethod("execute")
        executeMethod.invoke(finalBuilder)
      } else {
        data.write.format("delta").save(path)
      }
    } catch {
      case _: ClassNotFoundException =>
        logger.warn("delta-spark library not found. Falling back to parquet format for merge.")
        logger.warn("Add io.delta:delta-spark to your Spark classpath for Delta Lake support.")
        data.write.format("parquet").mode(SaveMode.Overwrite).save(path)
    }
  }
}
