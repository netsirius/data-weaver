package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/** Writes data to Elasticsearch indices.
  *
  * Config:
  *   nodes      - Elasticsearch node URLs (e.g., "http://localhost:9200")
  *   index      - Index name
  *   saveMode   - "Append" (default) or "Overwrite"
  *   idColumn   - Column to use as document _id (optional)
  *
  * Uses Spark Elasticsearch connector if available, falls back to REST bulk API.
  */
class ElasticsearchSinkConnector extends SinkConnector {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newHttpClient()

  def connectorType: String = "Elasticsearch"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val nodes = config.getOrElse("nodes",
      throw new IllegalArgumentException("Elasticsearch: 'nodes' is required"))
    val index = config.getOrElse("index",
      throw new IllegalArgumentException("Elasticsearch: 'index' is required"))
    val saveMode = config.getOrElse("saveMode", "Append")
    val idColumn = config.get("idColumn")

    // Try Spark ES connector first
    try {
      val writer = data.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", nodes.replace("http://", "").replace("https://", ""))
        .option("es.resource", index)
        .option("es.nodes.wan.only", "true")

      idColumn.foreach(id => writer.option("es.mapping.id", id))

      if (saveMode.toLowerCase == "overwrite") {
        writer.mode("overwrite").save()
      } else {
        writer.mode("append").save()
      }
    } catch {
      case _: ClassNotFoundException =>
        logger.warn("Spark ES connector not found, using REST bulk API fallback")
        writeBulkREST(data, nodes, index, idColumn)
    }
  }

  /** Fallback: write via Elasticsearch REST bulk API. */
  private def writeBulkREST(df: DataFrame, nodes: String, index: String, idColumn: Option[String]): Unit = {
    val rows = df.toJSON.collect()
    val bulkBody = new StringBuilder()

    rows.foreach { json =>
      val action = idColumn match {
        case Some(_) => s"""{"index":{"_index":"$index"}}"""
        case None    => s"""{"index":{"_index":"$index"}}"""
      }
      bulkBody.append(action).append("\n").append(json).append("\n")
    }

    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$nodes/_bulk"))
      .header("Content-Type", "application/x-ndjson")
      .POST(HttpRequest.BodyPublishers.ofString(bulkBody.toString()))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
      throw new RuntimeException(s"Elasticsearch bulk write failed (${response.statusCode()}): ${response.body().take(500)}")
    }

    logger.info(s"Wrote ${rows.length} documents to Elasticsearch index '$index'")
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val nodes = config.getOrElse("nodes", return Left("nodes not configured"))
    try {
      val start = System.currentTimeMillis()
      val request = HttpRequest.newBuilder()
        .uri(URI.create(s"$nodes/_cluster/health"))
        .GET()
        .build()
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      val latency = System.currentTimeMillis() - start
      if (response.statusCode() < 400) Right(latency)
      else Left(s"Elasticsearch unhealthy (${response.statusCode()})")
    } catch {
      case e: Exception => Left(s"Elasticsearch unreachable: ${e.getMessage}")
    }
  }
}
