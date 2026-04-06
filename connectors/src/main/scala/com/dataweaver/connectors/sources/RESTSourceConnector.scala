package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.collection.mutable.ListBuffer

/** Generic REST API source connector with pagination and auth support.
  *
  * Config:
  *   baseUrl    - API base URL
  *   endpoint   - API endpoint path
  *   method     - HTTP method (default: GET)
  *   auth       - Auth type: "none", "bearer", "api-key" (default: "none")
  *   token      - Bearer token or API key value
  *   headerName - Custom header name for api-key auth (default: "X-API-Key")
  *   pagination - Pagination type: "none", "offset", "cursor" (default: "none")
  *   pageSize   - Items per page (default: 100)
  *   maxPages   - Max pages to fetch (default: 10)
  *   dataPath   - JSON path to data array (e.g., "results", "data.items")
  *   params     - Additional query params as key=value pairs separated by &
  */
class RESTSourceConnector extends SourceConnector {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newHttpClient()

  def connectorType: String = "REST"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val baseUrl = config.getOrElse("baseUrl",
      throw new IllegalArgumentException("REST: 'baseUrl' is required"))
    val endpoint = config.getOrElse("endpoint", "")
    val method = config.getOrElse("method", "GET")
    val pagination = config.getOrElse("pagination", "none")
    val pageSize = config.getOrElse("pageSize", "100").toInt
    val maxPages = config.getOrElse("maxPages", "10").toInt
    val dataPath = config.get("dataPath")
    val params = config.getOrElse("params", "")

    val allData = ListBuffer[String]()

    pagination match {
      case "offset" =>
        var offset = 0
        var page = 0
        var hasMore = true
        while (hasMore && page < maxPages) {
          val separator = if (params.nonEmpty || endpoint.contains("?")) "&" else "?"
          val paginationParams = s"${separator}offset=$offset&limit=$pageSize"
          val url = s"$baseUrl$endpoint${if (params.nonEmpty) s"?$params" else ""}$paginationParams"
          val response = fetchUrl(url, method, config)
          allData += response
          page += 1
          offset += pageSize
          // Simple heuristic: if response is small, assume last page
          hasMore = response.length > 100
        }

      case "cursor" =>
        var cursor: Option[String] = None
        var page = 0
        var hasMore = true
        while (hasMore && page < maxPages) {
          val cursorParam = cursor.map(c => s"&cursor=$c").getOrElse("")
          val separator = if (params.nonEmpty || endpoint.contains("?")) "&" else "?"
          val url = s"$baseUrl$endpoint${if (params.nonEmpty) s"?$params" else ""}${separator}limit=$pageSize$cursorParam"
          val response = fetchUrl(url, method, config)
          allData += response
          page += 1
          // Extract next cursor from response (simple pattern)
          val cursorPattern = """"next_cursor"\s*:\s*"([^"]+)"""".r
          cursor = cursorPattern.findFirstMatchIn(response).map(_.group(1))
          hasMore = cursor.isDefined
        }

      case _ => // no pagination
        val url = s"$baseUrl$endpoint${if (params.nonEmpty) s"?$params" else ""}"
        allData += fetchUrl(url, method, config)
    }

    // Create DataFrame from JSON responses
    import spark.implicits._
    val jsonRDD = spark.sparkContext.parallelize(allData.toList)
    spark.read.option("multiLine", true).json(spark.createDataset(jsonRDD))
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val baseUrl = config.getOrElse("baseUrl", return Left("baseUrl not configured"))
    try {
      val start = System.currentTimeMillis()
      val request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl))
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .build()
      httpClient.send(request, HttpResponse.BodyHandlers.discarding())
      Right(System.currentTimeMillis() - start)
    } catch {
      case e: Exception => Left(s"REST API unreachable: ${e.getMessage}")
    }
  }

  private def fetchUrl(url: String, method: String, config: Map[String, String]): String = {
    logger.info(s"Fetching: $url")
    val builder = HttpRequest.newBuilder().uri(URI.create(url))

    // Apply auth
    config.getOrElse("auth", "none") match {
      case "bearer" =>
        val token = config.getOrElse("token", "")
        builder.header("Authorization", s"Bearer $token")
      case "api-key" =>
        val headerName = config.getOrElse("headerName", "X-API-Key")
        val token = config.getOrElse("token", "")
        builder.header(headerName, token)
      case _ => // no auth
    }

    builder.header("Accept", "application/json")

    val request = method.toUpperCase match {
      case "GET"  => builder.GET().build()
      case "POST" => builder.POST(HttpRequest.BodyPublishers.noBody()).build()
      case _      => builder.GET().build()
    }

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
      throw new RuntimeException(s"API error ${response.statusCode()}: ${response.body().take(500)}")
    }
    response.body()
  }
}
