package com.dataweaver.cli.commands

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

/** Scaffolds a new Data Weaver project with example pipeline and config. */
object InitCommand {

  def run(projectName: String): Unit = {
    val projectDir = Paths.get(projectName)

    if (Files.exists(projectDir)) {
      println(s"Project '$projectName' already exists.")
      return
    }

    val pipelinesDir = projectDir.resolve("pipelines")
    val configDir = projectDir.resolve("config")

    Files.createDirectories(pipelinesDir)
    Files.createDirectories(configDir)

    // Example pipeline
    writeFile(pipelinesDir.resolve("example_pipeline.yaml").toFile,
      """name: ExamplePipeline
        |tag: example
        |engine: auto
        |
        |dataSources:
        |  - id: source1
        |    type: File
        |    config:
        |      path: data/input.csv
        |      format: csv
        |
        |transformations:
        |  - id: filtered
        |    type: SQL
        |    sources:
        |      - source1
        |    query: >
        |      SELECT *
        |      FROM source1
        |      WHERE active = true
        |
        |  - id: quality_check
        |    type: DataQuality
        |    sources:
        |      - filtered
        |    checks:
        |      - row_count > 0
        |    onFail: warn
        |
        |sinks:
        |  - id: output
        |    type: File
        |    source: quality_check
        |    config:
        |      path: data/output
        |      format: parquet
        |      saveMode: Overwrite
        |
        |profiles:
        |  dev:
        |    engine: local
        |  prod:
        |    engine: spark
        |
        |tests:
        |  - name: "has data"
        |    assert: output.row_count > 0
        |""".stripMargin)

    // Connections file
    writeFile(configDir.resolve("connections.yaml").toFile,
      """connections:
        |  db-prod:
        |    type: PostgreSQL
        |    host: ${env.DB_HOST}
        |    port: ${env.DB_PORT}
        |    database: ${env.DB_NAME}
        |    user: ${env.DB_USER}
        |    password: ${env.DB_PASSWORD}
        |""".stripMargin)

    // .env example
    writeFile(projectDir.resolve(".env.example").toFile,
      """# Copy to .env and fill in values (gitignored)
        |DB_HOST=localhost
        |DB_PORT=5432
        |DB_NAME=mydb
        |DB_USER=myuser
        |DB_PASSWORD=mypassword
        |""".stripMargin)

    // .gitignore
    writeFile(projectDir.resolve(".gitignore").toFile,
      """.env
        |**/target/
        |*.class
        |.DS_Store
        |""".stripMargin)

    println(s"Project '$projectName' created successfully.")
    println()
    println("  Next steps:")
    println(s"    cd $projectName")
    println(s"    weaver validate pipelines/example_pipeline.yaml")
    println(s"    weaver plan pipelines/example_pipeline.yaml")
    println(s"    weaver apply pipelines/example_pipeline.yaml")
    println()
  }

  private def writeFile(file: File, content: String): Unit = {
    val writer = new PrintWriter(file)
    try writer.write(content)
    finally writer.close()
  }
}
