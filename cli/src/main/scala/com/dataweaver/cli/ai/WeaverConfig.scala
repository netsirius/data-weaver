package com.dataweaver.cli.ai

import scala.io.Source
import scala.util.Try

/** Loads ~/.weaver/config.yaml for AI settings.
  *
  * Expected format:
  * {{{
  * ai:
  *   provider: claude          # claude | openai
  *   apiKey: ${env.ANTHROPIC_API_KEY}
  *   model: claude-sonnet-4-20250514
  *   maxRetries: 3
  * }}}
  */
object WeaverConfig {

  case class AIConfig(
      provider: String = "claude",
      apiKey: String = "",
      model: String = "claude-sonnet-4-20250514",
      maxRetries: Int = 3
  )

  private val configPath = sys.props.getOrElse("user.home", ".") + "/.weaver/config.yaml"

  /** Load AI configuration from ~/.weaver/config.yaml.
    * Falls back to environment variables if config file doesn't exist.
    */
  def loadAIConfig(): AIConfig = {
    val fromFile = loadFromFile(configPath)

    // Resolve API key from env if it's a ${env.X} reference or empty
    val apiKey = resolveApiKey(fromFile.apiKey, fromFile.provider)

    fromFile.copy(apiKey = apiKey)
  }

  private def loadFromFile(path: String): AIConfig = {
    Try {
      val content = Source.fromFile(path).mkString
      val lines = content.split('\n').map(_.trim)

      var provider = "claude"
      var apiKey = ""
      var model = "claude-sonnet-4-20250514"
      var maxRetries = 3

      lines.foreach {
        case l if l.startsWith("provider:") => provider = l.split(":", 2)(1).trim.split("#")(0).trim
        case l if l.startsWith("apiKey:") => apiKey = l.split(":", 2)(1).trim
        case l if l.startsWith("model:") => model = l.split(":", 2)(1).trim.split("#")(0).trim
        case l if l.startsWith("maxRetries:") =>
          maxRetries = Try(l.split(":", 2)(1).trim.toInt).getOrElse(3)
        case _ =>
      }

      AIConfig(provider, apiKey, model, maxRetries)
    }.getOrElse(AIConfig())
  }

  private def resolveApiKey(key: String, provider: String): String = {
    val envPattern = """\$\{env\.([^}]+)\}""".r

    envPattern.findFirstMatchIn(key) match {
      case Some(m) =>
        sys.env.getOrElse(m.group(1), "")
      case None if key.nonEmpty =>
        key
      case _ =>
        // Fallback to standard env vars
        provider match {
          case "claude" => sys.env.getOrElse("ANTHROPIC_API_KEY", "")
          case "openai" => sys.env.getOrElse("OPENAI_API_KEY", "")
          case _        => ""
        }
    }
  }
}
