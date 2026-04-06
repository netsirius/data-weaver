package com.dataweaver.transformations.rag

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/** Generates vector embeddings by calling an embedding API.
  * Returns the input DataFrame with an additional "embedding" column (array of doubles).
  *
  * Config (via extra map):
  *   provider    - vertex-ai | openai | cohere (default: openai)
  *   model       - embedding model name (default: text-embedding-3-small)
  *   textColumn  - column containing text to embed (default: "text")
  *   batchSize   - texts per API call (default: 10)
  *   apiKey      - API key (or use env var)
  */
class EmbeddingPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newHttpClient()

  def transformType: String = "Embedding"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val df = inputs.values.headOption.getOrElse(
      throw new IllegalArgumentException(s"Embedding '${config.id}' requires input"))

    val provider = config.extra.getOrElse("provider", "openai")
    val model = config.extra.getOrElse("model", defaultModel(provider))
    val textColumn = config.extra.getOrElse("textColumn", "text")
    val batchSize = config.extra.getOrElse("batchSize", "10").toInt
    val apiKey = config.extra.getOrElse("apiKey", resolveApiKey(provider))

    // Collect texts
    val rows = df.collect().toList
    val textIdx = df.schema.fieldIndex(textColumn)

    // Generate embeddings in batches
    val allEmbeddings = rows.grouped(batchSize).flatMap { batch =>
      val texts = batch.map(r => Option(r.get(textIdx)).map(_.toString).getOrElse(""))
      callEmbeddingAPI(texts.toList, provider, model, apiKey)
    }.toList

    // Create output DataFrame with embedding column
    val outputRows = rows.zip(allEmbeddings).map { case (row, embedding) =>
      Row.fromSeq(row.toSeq :+ embedding)
    }

    val embeddingField = StructField("embedding", ArrayType(DoubleType), nullable = true)
    val outputSchema = StructType(df.schema.fields :+ embeddingField)

    spark.createDataFrame(spark.sparkContext.parallelize(outputRows), outputSchema)
  }

  private def callEmbeddingAPI(
      texts: List[String], provider: String, model: String, apiKey: String
  ): List[Seq[Double]] = {
    provider match {
      case "openai" => callOpenAIEmbedding(texts, model, apiKey)
      case _        => callOpenAIEmbedding(texts, model, apiKey) // default to OpenAI-compatible
    }
  }

  private def callOpenAIEmbedding(texts: List[String], model: String, apiKey: String): List[Seq[Double]] = {
    val escapedTexts = texts.map(t =>
      t.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ").take(8000))
    val inputArray = escapedTexts.map(t => s""""$t"""").mkString(",")
    val body = s"""{"model":"$model","input":[$inputArray]}"""

    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.openai.com/v1/embeddings"))
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer $apiKey")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
      throw new RuntimeException(s"Embedding API error (${response.statusCode()}): ${response.body().take(500)}")
    }

    // Parse embeddings from response
    parseEmbeddings(response.body(), texts.size)
  }

  /** Simple embedding parser — extracts arrays of floats from JSON response. */
  private def parseEmbeddings(json: String, expectedCount: Int): List[Seq[Double]] = {
    val embeddingPattern = """\[(-?[\d.]+(?:,-?[\d.]+)*)\]""".r
    val allMatches = embeddingPattern.findAllMatchIn(json).toList

    // Skip the first match (it's the input array), take embedding arrays
    val embeddings = allMatches.drop(1).take(expectedCount).map { m =>
      m.group(1).split(",").map(_.trim.toDouble).toSeq
    }

    // Pad with empty embeddings if needed
    embeddings.toList.padTo(expectedCount, Seq.empty[Double])
  }

  private def defaultModel(provider: String): String = provider match {
    case "openai"    => "text-embedding-3-small"
    case "vertex-ai" => "text-embedding-004"
    case "cohere"    => "embed-english-v3.0"
    case _           => "text-embedding-3-small"
  }

  private def resolveApiKey(provider: String): String = provider match {
    case "openai"    => sys.env.getOrElse("OPENAI_API_KEY", "")
    case "vertex-ai" => sys.env.getOrElse("GOOGLE_API_KEY", "")
    case "cohere"    => sys.env.getOrElse("COHERE_API_KEY", "")
    case _           => ""
  }
}
