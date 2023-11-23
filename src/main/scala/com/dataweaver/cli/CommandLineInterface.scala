package com.dataweaver.cli

import com.dataweaver.config.DataWeaverConfig
import com.dataweaver.runners.{LocalSparkRunner, RemoteSparkRunner}
import scopt.OParser

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.util.{Failure, Success, Try}

/**
 * Parses command line arguments for the DataWeaver application.
 * Acts as the entry point when the application is executed via the command line.
 */
object CommandLineInterface {

  case class Config(
                     command: String = "",
                     regex: Option[String] = None,
                     projectName: Option[String] = None,
                     runner: String = "local",
                     appName: String = "DataWeaver",
                     tag: Option[String] = None,
                     init: Boolean = false // Nueva opción para el comando "init"
                   )

  /**
   * The main entry point for the CLI.
   *
   * @param args Command line arguments.
   */
  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(config) => executeCommand(config)
      case None => println("Invalid command or arguments.")
    }
  }

  /**
   * Executes the command based on the parsed configuration.
   *
   * @param config Configuration parsed from command line arguments.
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
        DataWeaverConfig.load() match {
          case Success(project_config) =>
            val pipelinesDir = project_config.getPipelinesDir
            val runner_type: String = config.runner

            val runner = runner_type match {
              case "local" => new LocalSparkRunner
              case "remote" =>
                val waver_jar_path = Try(project_config.getWaverJarPath) match {
                  case Success(path) => path
                  case Failure(ex) =>
                    println(s"Error loading configuration: ${ex.getMessage}")
                    return
                }
                val waver_cluster_url = Try(project_config.getClusterUrl) match {
                  case Success(path) => path
                  case Failure(ex) =>
                    println(s"Error loading configuration: ${ex.getMessage}")
                    return
                }
                new RemoteSparkRunner(waver_jar_path, waver_cluster_url, config.appName)
              case _ => throw new IllegalArgumentException("Unspecified or incorrect runner type")
            }
            runner.run(pipelinesDir, config.tag, config.regex)
          case Failure(ex) =>
            println(s"Error loading configuration: ${ex.getMessage}")
        }
      case _ => println("Unknown command.")
    }
  }

  /**
   * Parses the command line arguments.
   *
   * @param args Command line arguments.
   * @return Configuration object based on the parsed arguments.
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
            opt[String]("runner")
              .optional()
              .action((x, c) => c.copy(runner = x))
              .text("Specifies the runner type: local or remote."),
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

  /**
   * Creates the initial structure for a new project.
   *
   * This function sets up a directory with the given project name and
   * necessary subdirectories for the project. It also generates
   * a sample pipeline file and a configuration file.
   *
   * @param projectName The name of the project. It will be used to name the main project directory.
   */
  private def createProjectStructure(projectName: String): Unit = {
    val projectDir = Paths.get(projectName)
    val pipelinesDir = projectDir.resolve("pipelines")

    if (Files.exists(projectDir)) {
      println(s"The project '$projectName' already exists.")
      return
    }

    Files.createDirectories(pipelinesDir)

    val pipelineExampleFile = pipelinesDir.resolve("pipeline_example.yaml").toFile
    val pipelineExampleContent =
      """# Example pipeline
      # Configure your pipeline steps here
      val pipelineExampleContent =
        \"\"\"name: ExamplePipeline
          |tag: example
          |dataSources:
          |  - id: source1
          |    type: MySQL
          |    config:
          |      connectionString: jdbc:mysql://localhost:3306/mydb
          |      tableName: mytable
          |transformations:
          |  - id: transform1
          |    type: SQLTransformation
          |    sources:
          |      - source1
          |    targetId: transformedData
          |    query: SELECT * FROM source1 WHERE column1 = 'value'
          |sinks:
          |  - id: sink1
          |    type: BigQuery
          |    config:
          |      projectId: my-project
          |      datasetName: my-dataset
          |      tableName: my-table
          |      temporaryGcsBucket: my-bucket
          |\"\"\".stripMargin
      """
    writeToFile(pipelineExampleFile, pipelineExampleContent)

    // Create weaver_project.conf file
    val configFile = projectDir.resolve("weaver_project.conf").toFile
    val configContent =
      s"""weaver {
      pipelines_dir = "${pipelinesDir.toAbsolutePath}"
      default_runner = "local"  # Or 'remote', depending on your needs
    }
    """
    writeToFile(configFile, configContent)

    println(s"Project '$projectName' successfully created.")
  }


  /**
   * Writes content to a specified file.
   *
   * This function takes a file and a string of text, writing
   * the content to the file. If the file does not exist, it is created.
   *
   * @param file    The file to which the content will be written.
   * @param content The text content to be written into the file.
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
