package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Reads data from MongoDB collections via Spark MongoDB connector.
  *
  * Config:
  *   uri        - MongoDB connection URI (e.g., "mongodb://host:27017")
  *   database   - Database name
  *   collection - Collection name
  *   pipeline   - Optional aggregation pipeline JSON (e.g., "[{$match: {active: true}}]")
  *
  * Requires: org.mongodb.spark:mongo-spark-connector on the Spark classpath.
  */
class MongoDBSourceConnector extends SourceConnector {
  def connectorType: String = "MongoDB"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val uri = config.getOrElse("uri",
      throw new IllegalArgumentException("MongoDB: 'uri' is required"))
    val database = config.getOrElse("database",
      throw new IllegalArgumentException("MongoDB: 'database' is required"))
    val collection = config.getOrElse("collection",
      throw new IllegalArgumentException("MongoDB: 'collection' is required"))

    val reader = spark.read
      .format("mongodb")
      .option("connection.uri", uri)
      .option("database", database)
      .option("collection", collection)

    config.get("pipeline").foreach(p => reader.option("aggregation.pipeline", p))

    reader.load()
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val uri = config.getOrElse("uri", return Left("uri not configured"))
    try {
      val start = System.currentTimeMillis()
      // Simple TCP check on MongoDB port
      val uriParts = uri.replace("mongodb://", "").split(":")
      val host = uriParts(0)
      val port = if (uriParts.length > 1) uriParts(1).split("/")(0).toInt else 27017
      val socket = new java.net.Socket()
      socket.connect(new java.net.InetSocketAddress(host, port), 5000)
      val latency = System.currentTimeMillis() - start
      socket.close()
      Right(latency)
    } catch {
      case e: Exception => Left(s"MongoDB connection failed: ${e.getMessage}")
    }
  }
}
