package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Reads data from Apache Kafka topics.
  * Supports both batch and streaming modes.
  *
  * Config:
  *   brokers       - Kafka broker list (e.g., "localhost:9092")
  *   topic         - Topic to read from
  *   startingOffsets - "earliest", "latest", or JSON offsets (default: "latest")
  *   mode          - "batch" (default) or "streaming"
  *   maxOffsetsPerTrigger - max records per micro-batch in streaming (optional)
  */
class KafkaSourceConnector extends SourceConnector {
  def connectorType: String = "Kafka"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val brokers = config.getOrElse("brokers",
      throw new IllegalArgumentException("Kafka: 'brokers' is required"))
    val topic = config.getOrElse("topic",
      throw new IllegalArgumentException("Kafka: 'topic' is required"))
    val startingOffsets = config.getOrElse("startingOffsets", "latest")
    val mode = config.getOrElse("mode", "batch")

    mode match {
      case "streaming" =>
        val reader = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", brokers)
          .option("subscribe", topic)
          .option("startingOffsets", startingOffsets)

        config.get("maxOffsetsPerTrigger").foreach(v => reader.option("maxOffsetsPerTrigger", v))

        reader.load()
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

      case _ => // batch
        spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", brokers)
          .option("subscribe", topic)
          .option("startingOffsets", startingOffsets)
          .load()
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
    }
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val brokers = config.getOrElse("brokers", return Left("brokers not configured"))
    try {
      import java.util.Properties
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("request.timeout.ms", "5000")
      props.put("default.api.timeout.ms", "5000")

      val start = System.currentTimeMillis()
      val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[String, String](props,
        new org.apache.kafka.common.serialization.StringDeserializer(),
        new org.apache.kafka.common.serialization.StringDeserializer())
      consumer.listTopics()
      val latency = System.currentTimeMillis() - start
      consumer.close()
      Right(latency)
    } catch {
      case e: Exception => Left(s"Kafka connection failed: ${e.getMessage}")
    }
  }
}
