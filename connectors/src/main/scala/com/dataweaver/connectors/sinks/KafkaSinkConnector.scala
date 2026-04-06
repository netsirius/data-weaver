package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/** Writes data to Apache Kafka topics.
  * Supports both batch and streaming write modes.
  *
  * Config:
  *   brokers     - Kafka broker list
  *   topic       - Topic to write to
  *   mode        - "batch" (default) or "streaming"
  *   keyColumn   - Column to use as Kafka key (optional)
  *   valueColumn - Column to use as Kafka value (default: all columns as JSON)
  *   checkpointLocation - Required for streaming mode
  *   triggerInterval    - Streaming trigger interval (e.g., "10 seconds")
  */
class KafkaSinkConnector extends SinkConnector {
  def connectorType: String = "Kafka"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val brokers = config.getOrElse("brokers",
      throw new IllegalArgumentException("Kafka sink: 'brokers' is required"))
    val topic = config.getOrElse("topic",
      throw new IllegalArgumentException("Kafka sink: 'topic' is required"))
    val mode = config.getOrElse("mode", "batch")

    val keyCol = config.get("keyColumn")
    val valueCol = config.getOrElse("valueColumn", "value")

    // Prepare DataFrame with key/value columns for Kafka
    val kafkaDf = if (data.columns.contains("value")) {
      data
    } else {
      import org.apache.spark.sql.functions.{struct, to_json, col}
      val valueExpr = to_json(struct(data.columns.map(col): _*))
      val withValue = data.withColumn("value", valueExpr)
      keyCol match {
        case Some(k) => withValue.withColumnRenamed(k, "key")
        case None    => withValue
      }
    }

    mode match {
      case "streaming" =>
        val checkpoint = config.getOrElse("checkpointLocation",
          s"/tmp/weaver-checkpoint/$pipelineName")
        val trigger = config.getOrElse("triggerInterval", "10 seconds")

        kafkaDf.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", brokers)
          .option("topic", topic)
          .option("checkpointLocation", checkpoint)
          .trigger(Trigger.ProcessingTime(trigger))
          .start()
          .awaitTermination()

      case _ => // batch
        kafkaDf.selectExpr("CAST(value AS STRING) AS value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", brokers)
          .option("topic", topic)
          .save()
    }
  }
}
