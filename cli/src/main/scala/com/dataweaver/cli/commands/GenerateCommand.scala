package com.dataweaver.cli.commands

import com.dataweaver.cli.ai.{LLMClient, PromptBuilder, WeaverConfig}
import com.dataweaver.core.config.{SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.DAGResolver

import java.io.{File, PrintWriter}

/** Generates pipeline YAML from natural language using an LLM.
  * Implements a validation loop: generate → validate → retry (max N attempts).
  */
object GenerateCommand {

  def run(description: String, outputPath: Option[String] = None): Unit = {
    val config = WeaverConfig.loadAIConfig()

    if (config.apiKey.isEmpty) {
      System.err.println("ERROR: No AI API key configured.")
      System.err.println()
      System.err.println("Configure it in ~/.weaver/config.yaml:")
      System.err.println("  ai:")
      System.err.println("    provider: claude")
      System.err.println("    apiKey: ${env.ANTHROPIC_API_KEY}")
      System.err.println()
      System.err.println("Or set the environment variable:")
      System.err.println("  export ANTHROPIC_API_KEY=your-key-here")
      return
    }

    println(s"Generating pipeline from: \"$description\"")
    println(s"Using: ${config.provider} / ${config.model}")
    println()

    // Build initial prompt
    val prompt = PromptBuilder.buildGeneratePrompt(description)

    // Generation + validation loop
    var yaml = ""
    var attempt = 0
    var valid = false

    while (attempt < config.maxRetries && !valid) {
      attempt += 1
      println(s"  Attempt $attempt/${config.maxRetries}...")

      val currentPrompt = if (attempt == 1) prompt
        else PromptBuilder.buildFixPrompt(yaml, getErrors(yaml))

      LLMClient.generate(currentPrompt, config) match {
        case Right(generated) =>
          yaml = cleanYaml(generated)
          val errors = getErrors(yaml)
          if (errors.isEmpty) {
            valid = true
            println(s"  ✓ Valid pipeline generated!")
          } else {
            println(s"  ✗ Validation failed (${errors.size} errors)")
            errors.foreach(e => println(s"    - $e"))
          }
        case Left(error) =>
          println(s"  ✗ LLM error: $error")
          return
      }
    }

    if (!valid) {
      System.err.println()
      System.err.println(s"Failed to generate valid pipeline after ${config.maxRetries} attempts.")
      System.err.println("The best attempt was:")
      System.err.println()
      println(yaml)
      return
    }

    // Output
    val outFile = outputPath.getOrElse(inferOutputPath(description))
    val writer = new PrintWriter(new File(outFile))
    try writer.write(yaml)
    finally writer.close()

    println()
    println(s"  Pipeline saved to: $outFile")
    println()
    println("  Next steps:")
    println(s"    weaver validate $outFile")
    println(s"    weaver plan $outFile")
    println(s"    weaver apply $outFile")
    println()
  }

  /** Validate YAML and return error list. */
  private def getErrors(yaml: String): List[String] = {
    YAMLParser.parseString(yaml) match {
      case Left(parseError) => List(parseError)
      case Right(config) =>
        val schemaErrors = SchemaValidator.validate(config)
        if (schemaErrors.nonEmpty) return schemaErrors
        try {
          DAGResolver.resolve(config)
          List.empty
        } catch {
          case e: Exception => List(e.getMessage)
        }
    }
  }

  /** Clean LLM output — remove markdown fences and leading/trailing whitespace. */
  private def cleanYaml(raw: String): String = {
    var cleaned = raw.trim
    // Remove markdown code fences
    if (cleaned.startsWith("```yaml")) cleaned = cleaned.drop(7)
    else if (cleaned.startsWith("```")) cleaned = cleaned.drop(3)
    if (cleaned.endsWith("```")) cleaned = cleaned.dropRight(3)
    cleaned.trim
  }

  /** Infer output filename from description. */
  private def inferOutputPath(description: String): String = {
    val name = description
      .toLowerCase
      .replaceAll("[^a-z0-9\\s]", "")
      .trim
      .split("\\s+")
      .take(4)
      .mkString("_")
    s"${name}_pipeline.yaml"
  }
}
