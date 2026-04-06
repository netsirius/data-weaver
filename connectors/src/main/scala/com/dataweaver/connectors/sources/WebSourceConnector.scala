package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.collection.mutable.ListBuffer

/** Fetches web pages and returns their HTML content as a DataFrame.
  * Designed for web scraping pipelines where the HTML is then parsed by LLMTransform.
  *
  * Config:
  *   urls       - Comma-separated list of URLs to fetch
  *   urlFile    - Path to a file with one URL per line (alternative to urls)
  *   userAgent  - Custom User-Agent header (default: "DataWeaver/0.2")
  *   delay      - Delay between requests in ms (default: 1000, be respectful)
  *   timeout    - Request timeout in seconds (default: 30)
  *
  * Returns DataFrame with columns: url, html, status_code, fetched_at
  */
class WebSourceConnector extends SourceConnector {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newBuilder()
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  def connectorType: String = "Web"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val urls = resolveUrls(config)
    val userAgent = config.getOrElse("userAgent", "DataWeaver/0.2")
    val delay = config.getOrElse("delay", "1000").toLong
    val timeout = config.getOrElse("timeout", "30").toInt

    if (urls.isEmpty) {
      throw new IllegalArgumentException("Web source: provide 'urls' (comma-separated) or 'urlFile' (path to file)")
    }

    logger.info(s"Fetching ${urls.size} web page(s)...")

    val results = ListBuffer[(String, String, Int, String)]()

    urls.foreach { url =>
      try {
        val request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .header("User-Agent", userAgent)
          .header("Accept", "text/html,application/xhtml+xml,*/*")
          .timeout(java.time.Duration.ofSeconds(timeout))
          .GET()
          .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        results += ((url, response.body(), response.statusCode(), java.time.Instant.now().toString))
        logger.info(s"  Fetched: $url (${response.statusCode()}, ${response.body().length} chars)")

        if (delay > 0 && urls.size > 1) Thread.sleep(delay)
      } catch {
        case e: Exception =>
          logger.warn(s"  Failed: $url — ${e.getMessage}")
          results += ((url, s"ERROR: ${e.getMessage}", 0, java.time.Instant.now().toString))
      }
    }

    import spark.implicits._
    results.toList.toDF("url", "html", "status_code", "fetched_at")
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val urls = resolveUrls(config)
    if (urls.isEmpty) return Left("No URLs configured")
    val url = urls.head
    try {
      val start = System.currentTimeMillis()
      val request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .timeout(java.time.Duration.ofSeconds(5))
        .build()
      httpClient.send(request, HttpResponse.BodyHandlers.discarding())
      Right(System.currentTimeMillis() - start)
    } catch {
      case e: Exception => Left(s"Cannot reach $url: ${e.getMessage}")
    }
  }

  private def resolveUrls(config: Map[String, String]): List[String] = {
    config.get("urls").map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .orElse {
        config.get("urlFile").map { path =>
          scala.io.Source.fromFile(path).getLines()
            .map(_.trim)
            .filter(l => l.nonEmpty && !l.startsWith("#"))
            .toList
        }
      }
      .getOrElse(List.empty)
  }
}
