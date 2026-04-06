package com.dataweaver.transformations.rag

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/** Extracts entities and relationships from text using an LLM.
  * Returns a DataFrame with columns: source_entity, source_type, relation, target_entity, target_type
  *
  * Config (via extra map):
  *   provider    - claude | openai | local (default: claude)
  *   model       - model name
  *   textColumn  - column with text to analyze (default: "text")
  *   entityTypes - comma-separated entity types to extract (default: "Person,Organization,Product")
  *   apiKey      - API key (or env var, not needed for local)
  *   baseUrl     - custom API URL (for local/Ollama: http://localhost:11434/v1)
  */
class GraphExtractionPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newHttpClient()

  def transformType: String = "GraphExtraction"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val df = inputs.values.headOption.getOrElse(
      throw new IllegalArgumentException(s"GraphExtraction '${config.id}' requires input"))

    val provider = config.extra.getOrElse("provider", "claude")
    val model = config.extra.getOrElse("model", defaultModel(provider))
    val textColumn = config.extra.getOrElse("textColumn", "text")
    val entityTypes = config.extra.getOrElse("entityTypes", "Person,Organization,Product")
    val apiKey = config.extra.getOrElse("apiKey", resolveApiKey(provider))
    val baseUrl = config.extra.get("baseUrl")

    val textIdx = df.schema.fieldIndex(textColumn)
    val rows = df.collect().toList

    val allRelations = rows.flatMap { row =>
      val text = Option(row.get(textIdx)).map(_.toString).getOrElse("")
      if (text.isEmpty) List.empty
      else extractEntities(text, entityTypes, provider, model, apiKey, baseUrl)
    }

    // Create output DataFrame
    val schema = StructType(Seq(
      StructField("source_entity", StringType),
      StructField("source_type", StringType),
      StructField("relation", StringType),
      StructField("target_entity", StringType),
      StructField("target_type", StringType)
    ))

    val outputRows = allRelations.map { case (se, st, rel, te, tt) =>
      Row(se, st, rel, te, tt)
    }

    spark.createDataFrame(spark.sparkContext.parallelize(outputRows), schema)
  }

  private def extractEntities(
      text: String, entityTypes: String,
      provider: String, model: String, apiKey: String, baseUrl: Option[String]
  ): List[(String, String, String, String, String)] = {
    val prompt = s"""Extract entities and relationships from this text.
Entity types to look for: $entityTypes

Text: ${text.take(4000)}

Return ONLY a JSON array of objects with these fields:
- source_entity: the subject entity name
- source_type: entity type of the subject
- relation: the relationship (e.g., "works_at", "created", "part_of")
- target_entity: the object entity name
- target_type: entity type of the object

Example: [{"source_entity":"Alice","source_type":"Person","relation":"works_at","target_entity":"Acme","target_type":"Organization"}]

JSON array:"""

    val response = callLLM(prompt, provider, model, apiKey, baseUrl)
    parseRelations(response)
  }

  private def callLLM(
      prompt: String, provider: String, model: String, apiKey: String, baseUrl: Option[String]
  ): String = {
    val escapedPrompt = prompt.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

    val (url, headers, body) = provider match {
      case "local" =>
        val localUrl = baseUrl.getOrElse("http://localhost:11434/v1/chat/completions")
        (localUrl,
          Map("Content-Type" -> "application/json"),
          s"""{"model":"$model","messages":[{"role":"user","content":"$escapedPrompt"}]}""")
      case "claude" =>
        ("https://api.anthropic.com/v1/messages",
          Map("x-api-key" -> apiKey, "anthropic-version" -> "2023-06-01", "Content-Type" -> "application/json"),
          s"""{"model":"$model","max_tokens":4096,"messages":[{"role":"user","content":"$escapedPrompt"}]}""")
      case "gemini" | "vertex-ai" =>
        val geminiUrl = baseUrl.getOrElse(
          s"https://generativelanguage.googleapis.com/v1beta/models/$model:generateContent?key=$apiKey")
        (geminiUrl,
          Map("Content-Type" -> "application/json"),
          s"""{"contents":[{"parts":[{"text":"$escapedPrompt"}]}],"generationConfig":{"maxOutputTokens":4096}}""")
      case _ => // openai-compatible
        val apiUrl = baseUrl.getOrElse("https://api.openai.com/v1/chat/completions")
        (apiUrl,
          Map("Authorization" -> s"Bearer $apiKey", "Content-Type" -> "application/json"),
          s"""{"model":"$model","max_tokens":4096,"messages":[{"role":"user","content":"$escapedPrompt"}]}""")
    }

    val builder = HttpRequest.newBuilder().uri(URI.create(url))
      .POST(HttpRequest.BodyPublishers.ofString(body))
    headers.foreach { case (k, v) => builder.header(k, v) }

    val response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
      logger.error(s"LLM API error: ${response.body().take(300)}")
      return "[]"
    }

    val textPattern = """"(?:text|content)"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    textPattern.findFirstMatchIn(response.body())
      .map(_.group(1).replace("\\n", "\n").replace("\\\"", "\""))
      .getOrElse("[]")
  }

  private def parseRelations(json: String): List[(String, String, String, String, String)] = {
    // Simple regex-based JSON array parser
    val objectPattern = """\{[^}]*"source_entity"\s*:\s*"([^"]*)"[^}]*"source_type"\s*:\s*"([^"]*)"[^}]*"relation"\s*:\s*"([^"]*)"[^}]*"target_entity"\s*:\s*"([^"]*)"[^}]*"target_type"\s*:\s*"([^"]*)"""".r

    objectPattern.findAllMatchIn(json).map { m =>
      (m.group(1), m.group(2), m.group(3), m.group(4), m.group(5))
    }.toList
  }

  private def defaultModel(provider: String): String = provider match {
    case "claude"              => "claude-sonnet-4-20250514"
    case "openai"              => "gpt-4o-mini"
    case "gemini" | "vertex-ai" => "gemini-2.0-flash"
    case "local"               => "llama3"
    case _                     => "gpt-4o-mini"
  }

  private def resolveApiKey(provider: String): String = provider match {
    case "claude"              => sys.env.getOrElse("ANTHROPIC_API_KEY", "")
    case "openai"              => sys.env.getOrElse("OPENAI_API_KEY", "")
    case "gemini" | "vertex-ai" => sys.env.getOrElse("GOOGLE_API_KEY", sys.env.getOrElse("GEMINI_API_KEY", ""))
    case "local"               => ""
    case _                     => ""
  }
}
