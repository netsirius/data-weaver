package com.dataweaver.cli.wizard

import java.io.{File, PrintWriter}
import scala.io.StdIn

/** Step-by-step interactive pipeline generator. No LLM required.
  * Asks the user questions and generates valid YAML from the answers.
  */
object InteractiveWizard {

  def run(): Unit = {
    println()
    println("  Data Weaver — Interactive Pipeline Wizard")
    println("  " + "\u2500" * 45)
    println()

    // Pipeline name
    val name = ask("Pipeline name", "MyPipeline")
    val tag = ask("Tag (for filtering)", "daily")

    // Data sources
    println()
    println("  Data Sources:")
    val sourceType = choose("Source type", Seq("PostgreSQL", "MySQL", "File", "Test"))
    val sourceId = ask("Source ID", "source1")

    val sourceConfig = sourceType match {
      case "PostgreSQL" | "MySQL" =>
        val connection = ask("Connection name (from connections.yaml)", "db-prod")
        val query = ask("SQL query", "SELECT * FROM my_table")
        s"""    connection: $connection
    query: >
      $query
    config:
      readMode: ReadOnce"""

      case "File" =>
        val path = ask("File path", "data/input.csv")
        val format = choose("Format", Seq("csv", "parquet", "json"))
        s"""    config:
      path: $path
      format: $format"""

      case "Test" =>
        s"""    config:
      readMode: ReadOnce"""

      case _ => ""
    }

    // Transformations
    println()
    println("  Transformations:")
    val addTransform = askYesNo("Add a SQL transformation?", default = true)
    val transformYaml = if (addTransform) {
      val transformId = ask("Transform ID", "transformed")
      val query = ask("SQL query (use source ID as table name)", s"SELECT * FROM $sourceId WHERE active = true")
      s"""transformations:
  - id: $transformId
    type: SQL
    sources:
      - $sourceId
    query: >
      $query"""
    } else ""

    val lastId = if (addTransform) ask("(confirm transform ID)", "transformed") else sourceId

    // Quality checks
    println()
    val addQuality = askYesNo("Add data quality checks?", default = true)
    val qualityYaml = if (addQuality) {
      val qualityId = "quality_check"
      s"""  - id: $qualityId
    type: DataQuality
    sources:
      - $lastId
    checks:
      - row_count > 0
      - missing_count(id) = 0
    onFail: abort"""
    } else ""

    val sinkSource = if (addQuality) "quality_check" else lastId

    // Sinks
    println()
    println("  Output:")
    val sinkType = choose("Sink type", Seq("File", "DeltaLake", "BigQuery", "Test"))
    val sinkId = ask("Sink ID", "output")

    val sinkConfig = sinkType match {
      case "File" =>
        val path = ask("Output path", "output/result")
        val format = choose("Format", Seq("parquet", "csv", "json"))
        s"""    config:
      path: $path
      format: $format
      saveMode: Overwrite"""

      case "DeltaLake" =>
        val path = ask("Delta Lake path", "s3://warehouse/table")
        val saveMode = choose("Save mode", Seq("Overwrite", "Append", "merge"))
        val mergeConfig = if (saveMode == "merge") s"\n      mergeKey: ${ask("Merge key column", "id")}" else ""
        s"""    config:
      path: $path
      saveMode: $saveMode$mergeConfig"""

      case "BigQuery" =>
        val project = ask("GCP Project ID", "$${env.GCP_PROJECT}")
        val dataset = ask("Dataset name", "my_dataset")
        val table = ask("Table name", "my_table")
        s"""    connection: bq-prod
    config:
      projectId: $project
      datasetName: $dataset
      tableName: $table
      saveMode: Overwrite"""

      case "Test" =>
        s"""    config:
      saveMode: Overwrite"""

      case _ => ""
    }

    // Generate YAML
    val transformSection = if (transformYaml.nonEmpty || qualityYaml.nonEmpty) {
      val parts = Seq(transformYaml, qualityYaml).filter(_.nonEmpty)
      if (transformYaml.nonEmpty) parts.mkString("\n") else s"transformations:\n${parts.mkString("\n")}"
    } else ""

    val yaml = s"""name: $name
tag: $tag
engine: auto
dataSources:
  - id: $sourceId
    type: $sourceType
$sourceConfig
$transformSection
sinks:
  - id: $sinkId
    type: $sinkType
    source: $sinkSource
$sinkConfig
tests:
  - name: "has data"
    assert: $sinkId.row_count > 0
"""

    // Save
    println()
    println("  Generated Pipeline:")
    println("  " + "\u2500" * 45)
    println(yaml)

    val outputFile = ask("Save to file", s"${name.toLowerCase.replaceAll("\\s+", "_")}.yaml")
    val writer = new PrintWriter(new File(outputFile))
    try writer.write(yaml)
    finally writer.close()

    println()
    println(s"  ✓ Pipeline saved to: $outputFile")
    println()
    println("  Next steps:")
    println(s"    weaver validate $outputFile")
    println(s"    weaver plan $outputFile")
    println(s"    weaver apply $outputFile")
    println()
  }

  private def ask(prompt: String, default: String): String = {
    print(s"  $prompt [$default]: ")
    val input = StdIn.readLine()
    if (input == null || input.trim.isEmpty) default else input.trim
  }

  private def choose(prompt: String, options: Seq[String]): String = {
    println(s"  $prompt:")
    options.zipWithIndex.foreach { case (opt, i) => println(s"    ${i + 1}) $opt") }
    print(s"  Choice [1]: ")
    val input = StdIn.readLine()
    val idx = try {
      if (input == null || input.trim.isEmpty) 0 else input.trim.toInt - 1
    } catch { case _: Exception => 0 }
    options(idx.max(0).min(options.size - 1))
  }

  private def askYesNo(prompt: String, default: Boolean): Boolean = {
    val defaultStr = if (default) "Y/n" else "y/N"
    print(s"  $prompt [$defaultStr]: ")
    val input = StdIn.readLine()
    if (input == null || input.trim.isEmpty) default
    else input.trim.toLowerCase.startsWith("y")
  }
}
