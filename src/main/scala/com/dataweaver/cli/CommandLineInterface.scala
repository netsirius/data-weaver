package com.dataweaver.cli

import com.dataweaver.config.DataWeaverConfig
import com.dataweaver.runner.{LocalSparkRunner, RemoteSparkRunner}
import org.apache.log4j.LogManager
import scopt.OParser

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success}

/** Parses command line arguments for the DataWeaver application. Acts as the entry point when the
  * application is executed via the command line.
  */
object CommandLineInterface {

  private val logger = LogManager.getLogger(getClass)

  case class Config(
      command: String = "",
      regex: Option[String] = None,
      projectName: Option[String] = None,
      configPath: Option[String] = None,
      runner: String = "local",
      appName: String = "DataWeaver",
      executionMode: String = "Production",
      tag: Option[String] = None,
      init: Boolean = false
  )

  /** The main entry point for the CLI.
    *
    * @param args
    *   Command line arguments.
    */
  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(config) => executeCommand(config)
      case None         => println("Invalid command or arguments.")
    }
  }

  /** Executes the command based on the parsed configuration.
    *
    * @param config
    *   Configuration parsed from command line arguments.
    */
  private def executeCommand(config: Config): Unit = {
    config.command match {
      case "init" =>
        config.projectName match {
          case Some(projectName) =>
            createProjectStructure(projectName)
          case None =>
            println("Please specify a project name using --projectName.")
        }
      case "run" =>
        val configPath = config.configPath.getOrElse(System.getProperty("user.dir") + "/config")
        println(s"Loading configuration from $configPath")
        DataWeaverConfig.load(configPath) match {
          case Success(projectConfig) =>
            val runner_type: String = config.runner

            val runner = runner_type match {
              case "local" =>
                new LocalSparkRunner(config.executionMode, config.appName)
              case "remote" =>
                new RemoteSparkRunner(config.executionMode, config.appName)
              case _ => throw new IllegalArgumentException("Unspecified or incorrect runner type")
            }
            runner.run(configPath, config.tag, config.regex)
          case Failure(ex) =>
            println(s"Error loading configuration: ${ex.getMessage}")
        }
      case _ => println("Unknown command.")
    }
  }

  /** Parses the command line arguments.
    *
    * @param args
    *   Command line arguments.
    * @return
    *   Configuration object based on the parsed arguments.
    */
  private def parse(args: Array[String]): Option[Config] = {
    // Define the CLI parser
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("weaver"),
        head("DataWeaver CLI", "1.0"),
        cmd("init")
          .action((_, c) => c.copy(command = "init"))
          .text("Initialize a new project structure.") // Descripción del comando "init"
          .children(
            opt[String]("projectName")
              .required()
              .action((x, c) => c.copy(projectName = Some(x)))
              .text("Name of the project to initialize.") // Argumento para el nombre del proyecto
          ),
        cmd("run")
          .action((_, c) => c.copy(command = "run"))
          .text("Runs the data pipelines.")
          .children(
            opt[String]("configPath")
              .optional()
              .action((x, c) => c.copy(configPath = Some(x)))
              .text("Specifies the path to the project configuration file."),
            opt[String]("runner")
              .optional()
              .action((x, c) => c.copy(runner = x))
              .text("Specifies the runner type: local or remote."),
            opt[String]("executionMode")
              .optional()
              .action((x, c) => c.copy(executionMode = x))
              .text("Specifies the execution mode: pro or test."),
            opt[String]("appName")
              .optional()
              .action((x, c) => c.copy(appName = x))
              .text("Specifies the custom app name."),
            opt[String]("pipelines")
              .optional()
              .action((x, c) => c.copy(regex = Some(x)))
              .text("Regex to filter pipeline files."),
            opt[String]("tag")
              .optional()
              .valueName("<tag>")
              .action((x, c) => c.copy(tag = Some(x)))
              .text("Tag to filter specific pipelines.")
          )
      )
    }

    OParser.parse(parser, args, Config())
  }

  /** Creates the initial structure for a new project.
    *
    * This function sets up a directory with the given project name and necessary subdirectories for
    * the project. It also generates a sample pipeline file and a configuration file.
    *
    * @param projectName
    *   The name of the project. It will be used to name the main project directory.
    */
  private def createProjectStructure(projectName: String): Unit = {
    val projectDir = Paths.get(projectName)

    if (Files.exists(projectDir)) {
      println(s"The project '$projectName' already exists.")
      return
    }

    val pipelinesDir = projectDir.resolve("pipelines")
    val configDir = projectDir.resolve("config")

    Files.createDirectories(pipelinesDir)
    Files.createDirectories(configDir)

    val pipelineExampleFile = pipelinesDir.resolve("pipeline_example.yaml").toFile
    val pipelineExampleContent =
      """
        |name: ExamplePipeline
        |tag: example
        |dataSources:
        |  - id: testSource
        |    type: MySQL
        |    query: >
        |      SELECT name
        |      FROM test_table
        |    config:
        |      readMode: ReadOnce # ReadOnce, Incremental..
        |      connection: testConnection # Connection name related to the defined connections inside application.conf
        |transformations:
        |  - id: transform1
        |    type: SQLTransformation
        |    sources:
        |      - testSource # Source name related to the defined data sources inside pipeline.yaml
        |    query: >
        |      SELECT name as id
        |      FROM testSource
        |  - id: transform2
        |    type: SQLTransformation
        |    sources:
        |      - transform1 # Source name related to the defined data sources inside pipeline.yaml
        |    query: >
        |      SELECT id
        |      FROM transform1
        |      WHERE id = "Alice"
        |sinks:
        |  - id: sink1
        |    type: BigQuery
        |    config:
        |      saveMode: Append # Append, Overwrite, Merge...
        |      profile: testProfile # Profile name related to the defined profiles inside application.conf
        |""".stripMargin
    writeToFile(pipelineExampleFile, pipelineExampleContent)

    // Create application.conf file
    val configFile = configDir.resolve("application.conf").toFile
    val configContent =
      """
        |weaver {
        |    pipeline {
        |        pipelines_dir = "src/test/resources/test_project/pipelines"
        |        jar_path = "target/scala-2.13/data-weaver.jar"
        |        cluster_url = "local[2]"
        |    }
        |}
        |connections {
        |    testConnection {
        |        tableName = "testTable"
        |        host = "localhost"
        |        port = 5432
        |        database = "testing"
        |        user = "test"
        |        password = "test"
        |    }
        |}
        |profiles {
        |    testProfile {
        |        projectId = "test"
        |        datasetName = "test"
        |        tableName = "test"
        |        temporaryGcsBucket = "gs://test-data-weaver"
        |    }
        |}
        |""".stripMargin
    writeToFile(configFile, configContent)

    println(s"Project '$projectName' successfully created.")
  }

  /** Writes content to a specified file.
    *
    * This function takes a file and a string of text, writing the content to the file. If the file
    * does not exist, it is created.
    *
    * @param file
    *   The file to which the content will be written.
    * @param content
    *   The text content to be written into the file.
    */
  private def writeToFile(file: File, content: String): Unit = {
    val writer = new PrintWriter(file)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }
}
