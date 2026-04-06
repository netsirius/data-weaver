package com.dataweaver.cli.ai

import org.apache.log4j.LogManager

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.util.{Failure, Success, Try}

/** HTTP client for LLM APIs (Claude, OpenAI).
  * Uses java.net.http — no external HTTP library dependencies.
  */
object LLMClient {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newHttpClient()

  /** Call an LLM API and return the generated text.
    * @param prompt  The user prompt
    * @param config  AI configuration (provider, apiKey, model)
    * @return Right(generated text) or Left(error message)
    */
  def generate(prompt: String, config: WeaverConfig.AIConfig): Either[String, String] = {
    if (config.apiKey.isEmpty) {
      return Left(
        s"No API key configured for '${config.provider}'. " +
          s"Set it via ~/.weaver/config.yaml or environment variable " +
          s"(ANTHROPIC_API_KEY for Claude, OPENAI_API_KEY for OpenAI)")
    }

    config.provider match {
      case "claude" => callClaude(prompt, config)
      case "openai" => callOpenAI(prompt, config)
      case other    => Left(s"Unknown AI provider '$other'. Supported: claude, openai")
    }
  }

  private def callClaude(prompt: String, config: WeaverConfig.AIConfig): Either[String, String] = {
    val body = s"""{
      "model": "${config.model}",
      "max_tokens": 4096,
      "messages": [{"role": "user", "content": ${escapeJson(prompt)}}]
    }"""

    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.anthropic.com/v1/messages"))
      .header("Content-Type", "application/json")
      .header("x-api-key", config.apiKey)
      .header("anthropic-version", "2023-06-01")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    executeRequest(request).flatMap(extractClaudeResponse)
  }

  private def callOpenAI(prompt: String, config: WeaverConfig.AIConfig): Either[String, String] = {
    val body = s"""{
      "model": "${config.model}",
      "messages": [{"role": "user", "content": ${escapeJson(prompt)}}],
      "max_tokens": 4096
    }"""

    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.openai.com/v1/chat/completions"))
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer ${config.apiKey}")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    executeRequest(request).flatMap(extractOpenAIResponse)
  }

  private def executeRequest(request: HttpRequest): Either[String, String] = {
    Try(httpClient.send(request, HttpResponse.BodyHandlers.ofString())) match {
      case Success(response) if response.statusCode() >= 200 && response.statusCode() < 300 =>
        Right(response.body())
      case Success(response) =>
        Left(s"API error (${response.statusCode()}): ${response.body().take(500)}")
      case Failure(e) =>
        Left(s"Request failed: ${e.getMessage}")
    }
  }

  /** Extract text content from Claude API response JSON. */
  private def extractClaudeResponse(json: String): Either[String, String] = {
    // Simple JSON extraction without a JSON library
    val contentPattern = """"text"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    contentPattern.findFirstMatchIn(json) match {
      case Some(m) => Right(unescapeJson(m.group(1)))
      case None    => Left(s"Cannot parse Claude response: ${json.take(500)}")
    }
  }

  /** Extract text content from OpenAI API response JSON. */
  private def extractOpenAIResponse(json: String): Either[String, String] = {
    val contentPattern = """"content"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    contentPattern.findFirstMatchIn(json) match {
      case Some(m) => Right(unescapeJson(m.group(1)))
      case None    => Left(s"Cannot parse OpenAI response: ${json.take(500)}")
    }
  }

  private def escapeJson(s: String): String = {
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\""
  }

  private def unescapeJson(s: String): String = {
    s.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t").replace("\\\"", "\"").replace("\\\\", "\\")
  }
}
