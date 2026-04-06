package com.dataweaver.cli.ai

import scala.io.Source
import scala.util.Try

/** Builds schema-grounded prompts for LLM pipeline generation.
  * The prompt includes the JSON Schema so the LLM generates structurally valid YAML.
  */
object PromptBuilder {

  /** Build a prompt for generating a pipeline YAML from a natural language description.
    * @param description User's natural language description of the pipeline
    * @return Complete prompt with schema grounding
    */
  def buildGeneratePrompt(description: String): String = {
    val schema = loadSchema()

    s"""You are a data pipeline generator for Data Weaver, a declarative ETL framework.

Generate a valid YAML pipeline based on this description:
"$description"

IMPORTANT RULES:
1. Output ONLY the YAML content, no markdown fences, no explanation
2. The YAML must conform to this JSON Schema:

$schema

3. Available source types: PostgreSQL, MySQL, File, Test
4. Available sink types: BigQuery, DeltaLake, File, Test
5. Available transform types: SQL, DataQuality
6. Every sink MUST have a "source" field pointing to a transform id
7. Use SQL transforms with standard SQL queries
8. Add DataQuality checks where appropriate (row_count > 0, missing_count, duplicate_count)
9. Use $${env.VAR} for any credentials or sensitive values
10. Add inline tests to validate the pipeline output
11. Set engine to "auto"

CONNECTOR CONFIG REFERENCE:

PostgreSQL source config: host, port, database, user, password, query
MySQL source config: host, port, db, user, password, query, driver
File source config: path, format (csv|json|parquet|orc), header (true|false)
BigQuery sink config: projectId, datasetName, tableName, temporaryGcsBucket, saveMode
DeltaLake sink config: path, saveMode (Overwrite|Append|merge), mergeKey
File sink config: path, format (csv|json|parquet|orc), saveMode, coalesce, partitionBy

Generate the YAML now:"""
  }

  /** Build a prompt for fixing a pipeline that failed validation.
    * @param yaml      The invalid YAML
    * @param errors    Validation error messages
    * @return Prompt asking the LLM to fix the errors
    */
  def buildFixPrompt(yaml: String, errors: List[String]): String = {
    s"""The following Data Weaver pipeline YAML has validation errors. Fix them.

CURRENT YAML:
$yaml

ERRORS:
${errors.mkString("\n")}

RULES:
1. Output ONLY the corrected YAML, no explanation
2. Fix all listed errors
3. Do not change parts that are already correct
4. Every sink must have a "source" field
5. All source references must exist as dataSource or transformation ids

Generate the corrected YAML now:"""
  }

  /** Load the pipeline JSON Schema from resources. */
  private def loadSchema(): String = {
    Try {
      val stream = getClass.getClassLoader.getResourceAsStream("schemas/pipeline.schema.json")
      if (stream != null) {
        val source = Source.fromInputStream(stream)
        try source.mkString finally source.close()
      } else {
        // Fallback: read from file
        val source = Source.fromFile("core/src/main/resources/schemas/pipeline.schema.json")
        try source.mkString finally source.close()
      }
    }.getOrElse {
      // Minimal inline schema as last resort
      """{"type":"object","required":["name"],"properties":{"name":{"type":"string"},"dataSources":{"type":"array"},"transformations":{"type":"array"},"sinks":{"type":"array"}}}"""
    }
  }
}
