package com.dataweaver.cli

import com.dataweaver.config.AppConfig
import com.dataweaver.runners.{LocalSparkRunner, RemoteSparkRunner}
import scopt.OParser

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

case class Config(
                   command: String = "",
                   regex: Option[String] = None,
                   projectName: Option[String] = None,
                   runner: Option[String] = None,
                   tag: Option[String] = None,
                   init: Boolean = false // Nueva opción para el comando "init"
                 )


object CommandLineInterface {

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("weaver"),
        head("weaver", "1.0"),

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
          .text("Run pipelines.")
          .children(
            opt[String]("pipelines")
              .optional()
              .action((x, c) => c.copy(regex = Some(x)))
              .text("Regex to filter pipeline files."),
            opt[String]("tag")
              .optional()
              .action((x, c) => c.copy(tag = Some(x)))
              .text("Tag to filter pipelines.")
          ),
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.command match {
          case "init" =>
            config.projectName match {
              case Some(projectName) =>
                createProjectStructure(projectName)
              case None =>
                println("Please specify a project name using --projectName.")
            }
          case "run" =>
            val pipelinesFolder = AppConfig.getPipelinesDir(AppConfig.loadConfig.get)
            val runner_type: String = config.runner.getOrElse(AppConfig.getDefaultRunner(AppConfig.loadConfig.get))
            val runner = runner_type match {
              case "local" => new LocalSparkRunner
              case "remote" => new RemoteSparkRunner
              case _ => throw new IllegalArgumentException("Unspecified or incorrect runner type")
            }
            runner.run(pipelinesFolder, config.tag, config.regex)
        }
      case _ =>
    }
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

