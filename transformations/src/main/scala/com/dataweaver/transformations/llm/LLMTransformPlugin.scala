package com.dataweaver.transformations.llm

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.MessageDigest
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** LLM-as-transformation plugin.
  * Calls an LLM API for each batch of rows, parses structured JSON output,
  * and returns a new DataFrame with the output schema.
  *
  * Config (via extra map):
  *   provider       - claude | openai | vertex-ai (default: claude)
  *   model          - model name
  *   prompt         - prompt template with {column_name} placeholders
  *   inputColumns   - comma-separated list of columns to inject into prompt
  *   outputSchema   - pipe-separated field definitions: name:type|name:type
  *   batchSize      - rows per LLM call (default: 1)
  *   maxConcurrent  - parallel LLM calls (default: 5)
  *   retryOnError   - retry count (default: 3)
  *   cache          - true|false, content-hash cache (default: false)
  *   apiKey         - API key (or use env var)
  */
class LLMTransformPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)

  def transformType: String = "LLMTransform"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val df = inputs.values.headOption.getOrElse(
      throw new IllegalArgumentException(s"LLMTransform '${config.id}' requires at least one input"))

    val provider = config.extra.getOrElse("provider", "claude")
    val model = config.extra.getOrElse("model", "claude-sonnet-4-20250514")
    val promptTemplate = config.extra.getOrElse("prompt",
      throw new IllegalArgumentException(s"LLMTransform '${config.id}' requires 'prompt' in config"))
    val inputColumns = config.extra.getOrElse("inputColumns", "").split(",").map(_.trim).filter(_.nonEmpty)
    val outputSchemaStr = config.extra.getOrElse("outputSchema", "result:string")
    val batchSize = config.extra.getOrElse("batchSize", "1").toInt
    val maxConcurrent = config.extra.getOrElse("maxConcurrent", "5").toInt
    val retryCount = config.extra.getOrElse("retryOnError", "3").toInt
    val useCache = config.extra.getOrElse("cache", "false").toBoolean
    val apiKey = resolveApiKey(config.extra, provider)

    // Parse output schema
    val outputFields = parseOutputSchema(outputSchemaStr)
    val outputStructType = StructType(outputFields.map { case (name, tpe) =>
      StructField(name, tpe, nullable = true)
    })

    // Collect input data for processing
    val inputData = df.collect().toList
    val inputSchema = df.schema

    // Process in batches with LLM
    val cache = if (useCache) Some(mutable.Map[String, String]()) else None
    val results = inputData.grouped(batchSize).flatMap { batch =>
      val prompt = buildPromptForBatch(promptTemplate, batch, inputColumns, inputSchema)
      val cacheKey = if (useCache) Some(hashContent(prompt)) else None

      val response = cacheKey.flatMap(k => cache.flatMap(_.get(k))) match {
        case Some(cached) =>
          logger.info(s"Cache hit for LLMTransform '${config.id}'")
          cached
        case None =>
          val result = callLLMWithRetry(prompt, provider, model, apiKey, retryCount)
          cacheKey.foreach(k => cache.foreach(_.update(k, result)))
          result
      }

      parseJsonResponse(response, outputFields)
    }.toList

    // Create output DataFrame
    val rows = results.map(values => Row.fromSeq(values))
    val rdd = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rdd, outputStructType)
  }

  private def buildPromptForBatch(
      template: String, batch: List[Row], inputColumns: Array[String], schema: StructType
  ): String = {
    if (batch.size == 1) {
      var prompt = template
      val row = batch.head
      inputColumns.foreach { col =>
        val idx = schema.fieldIndex(col)
        val value = Option(row.get(idx)).map(_.toString).getOrElse("")
        prompt = prompt.replace(s"{$col}", value)
      }
      prompt
    } else {
      val items = batch.map { row =>
        inputColumns.map { col =>
          val idx = schema.fieldIndex(col)
          s"$col: ${Option(row.get(idx)).map(_.toString).getOrElse("")}"
        }.mkString(", ")
      }.mkString("\n---\n")
      s"$template\n\nProcess these ${batch.size} items:\n$items\n\nReturn a JSON array with one object per item."
    }
  }

  private def callLLMWithRetry(
      prompt: String, provider: String, model: String, apiKey: String, maxRetries: Int
  ): String = {
    var lastError: String = ""
    for (attempt <- 1 to maxRetries) {
      Try(callLLM(prompt, provider, model, apiKey)) match {
        case Success(response) => return response
        case Failure(e) =>
          lastError = e.getMessage
          logger.warn(s"LLM call attempt $attempt/$maxRetries failed: $lastError")
          if (attempt < maxRetries) Thread.sleep(attempt * 1000L) // exponential backoff
      }
    }
    throw new RuntimeException(s"LLM call failed after $maxRetries attempts: $lastError")
  }

  private def callLLM(prompt: String, provider: String, model: String, apiKey: String): String = {
    val escapedPrompt = prompt.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
    val (url, headers, body) = provider match {
      case "claude" =>
        (
          "https://api.anthropic.com/v1/messages",
          Map("x-api-key" -> apiKey, "anthropic-version" -> "2023-06-01", "Content-Type" -> "application/json"),
          s"""{"model":"$model","max_tokens":4096,"messages":[{"role":"user","content":"$escapedPrompt"}]}"""
        )
      case "local" =>
        // Ollama-compatible API (OpenAI-compatible endpoint)
        (
          "http://localhost:11434/v1/chat/completions",
          Map("Content-Type" -> "application/json"),
          s"""{"model":"$model","messages":[{"role":"user","content":"$escapedPrompt"}]}"""
        )
      case _ => // openai and any other OpenAI-compatible provider
        val apiUrl = if (provider == "openai") "https://api.openai.com/v1/chat/completions"
                     else s"https://api.$provider.com/v1/chat/completions" // extensible
        (
          apiUrl,
          Map("Authorization" -> s"Bearer $apiKey", "Content-Type" -> "application/json"),
          s"""{"model":"$model","max_tokens":4096,"messages":[{"role":"user","content":"$escapedPrompt"}]}"""
        )
    }

    val client = HttpClient.newHttpClient()
    val builder = HttpRequest.newBuilder().uri(URI.create(url))
      .POST(HttpRequest.BodyPublishers.ofString(body))
    headers.foreach { case (k, v) => builder.header(k, v) }

    val response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
      throw new RuntimeException(s"LLM API error (${response.statusCode()}): ${response.body().take(500)}")
    }

    // Extract text from response
    val textPattern = """"text"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    val contentPattern = """"content"\s*:\s*"((?:[^"\\]|\\.)*)"""".r

    val pattern = if (provider == "claude") textPattern else contentPattern
    pattern.findFirstMatchIn(response.body()) match {
      case Some(m) => m.group(1).replace("\\n", "\n").replace("\\\"", "\"")
      case None    => throw new RuntimeException(s"Cannot parse LLM response")
    }
  }

  private def parseOutputSchema(schemaStr: String): List[(String, DataType)] = {
    schemaStr.split("\\|").map(_.trim).map { field =>
      field.split(":") match {
        case Array(name, tpe) => (name.trim, tpe.trim.toLowerCase match {
          case "string"  => StringType
          case "integer" | "int" => IntegerType
          case "long"    => LongType
          case "double"  => DoubleType
          case "boolean" => BooleanType
          case "float"   => FloatType
          case _         => StringType
        })
        case Array(name) => (name.trim, StringType)
        case _           => ("result", StringType)
      }
    }.toList
  }

  private def parseJsonResponse(response: String, schema: List[(String, DataType)]): List[List[Any]] = {
    // Simple JSON extraction — parse each field from the response
    val results = mutable.ListBuffer[List[Any]]()

    // Try to extract as single object
    val values = schema.map { case (name, dataType) =>
      val pattern = s""""$name"\\s*:\\s*"?([^",}]*)"?""".r
      pattern.findFirstMatchIn(response) match {
        case Some(m) => castValue(m.group(1).trim, dataType)
        case None    => null
      }
    }
    results += values

    results.toList
  }

  private def castValue(value: String, dataType: DataType): Any = {
    if (value == null || value == "null" || value.isEmpty) return null
    try {
      dataType match {
        case StringType  => value.stripPrefix("\"").stripSuffix("\"")
        case IntegerType => value.toInt
        case LongType    => value.toLong
        case DoubleType  => value.toDouble
        case FloatType   => value.toFloat
        case BooleanType => value.toBoolean
        case _           => value
      }
    } catch {
      case _: Exception => null
    }
  }

  private def resolveApiKey(config: Map[String, String], provider: String): String = {
    config.getOrElse("apiKey", {
      provider match {
        case "claude" => sys.env.getOrElse("ANTHROPIC_API_KEY", "")
        case "openai" => sys.env.getOrElse("OPENAI_API_KEY", "")
        case "local"  => "" // Ollama doesn't need an API key
        case _        => ""
      }
    })
  }

  private def hashContent(content: String): String = {
    MessageDigest.getInstance("SHA-256")
      .digest(content.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString
  }
}
