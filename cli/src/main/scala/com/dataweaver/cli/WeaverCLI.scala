package com.dataweaver.cli

import com.dataweaver.cli.commands._
import com.dataweaver.cli.wizard.InteractiveWizard
import scopt.OParser

object WeaverCLI {

  case class Config(
      command: String = "",
      pipeline: Option[String] = None,
      env: Option[String] = None,
      inspectId: Option[String] = None,
      autoGenerate: Boolean = false,
      showCoverage: Boolean = false,
      description: Option[String] = None,
      interactive: Boolean = false,
      projectName: Option[String] = None
  )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("weaver"),
        head("Data Weaver", "0.2.0"),
        cmd("init")
          .action((_, c) => c.copy(command = "init"))
          .text("Initialize a new project or generate pipeline interactively")
          .children(
            arg[String]("<project-name>")
              .optional()
              .action((x, c) => c.copy(projectName = Some(x)))
              .text("Project name"),
            opt[Unit]("interactive")
              .action((_, c) => c.copy(interactive = true))
              .text("Step-by-step pipeline wizard (no LLM required)")
          ),
        cmd("generate")
          .action((_, c) => c.copy(command = "generate"))
          .text("Generate pipeline YAML from natural language description")
          .children(
            arg[String]("<description>")
              .action((x, c) => c.copy(description = Some(x)))
              .text("Natural language description of the pipeline")
          ),
        cmd("doctor")
          .action((_, c) => c.copy(command = "doctor"))
          .text("Full system diagnostic")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file")
          ),
        cmd("validate")
          .action((_, c) => c.copy(command = "validate"))
          .text("Validate a pipeline YAML file")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file")
          ),
        cmd("plan")
          .action((_, c) => c.copy(command = "plan"))
          .text("Dry-run: show what will be read/transformed/written")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file")
          ),
        cmd("explain")
          .action((_, c) => c.copy(command = "explain"))
          .text("Show resolved DAG and execution plan")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file")
          ),
        cmd("inspect")
          .action((_, c) => c.copy(command = "inspect"))
          .text("Show details of a source or transform")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file"),
            arg[String]("<id>")
              .action((x, c) => c.copy(inspectId = Some(x)))
              .text("ID of the source or transform to inspect")
          ),
        cmd("test")
          .action((_, c) => c.copy(command = "test"))
          .text("Run tests defined in pipeline YAML")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file"),
            opt[Unit]("auto-generate")
              .action((_, c) => c.copy(autoGenerate = true))
              .text("Auto-generate tests from schema inference"),
            opt[Unit]("coverage")
              .action((_, c) => c.copy(showCoverage = true))
              .text("Show test coverage report")
          ),
        cmd("apply")
          .action((_, c) => c.copy(command = "apply"))
          .text("Execute a pipeline")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file"),
            opt[String]("env")
              .action((x, c) => c.copy(env = Some(x)))
              .text("Environment profile (dev, prod)")
          )
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.command match {
          case "init" =>
            if (config.interactive) InteractiveWizard.run()
            else InitCommand.run(config.projectName.getOrElse("my-project"))
          case "generate" =>
            GenerateCommand.run(config.description.get)
          case "doctor" =>
            val result = DoctorCommand.run(config.pipeline.get)
            if (!result.overallHealthy) sys.exit(1)
          case "validate" =>
            val errors = ValidateCommand.run(config.pipeline.get)
            if (errors.nonEmpty) {
              errors.foreach(e => System.err.println(s"ERROR: $e"))
              sys.exit(1)
            }
          case "test" =>
            TestCommand.run(config.pipeline.get, config.autoGenerate, config.showCoverage)
          case "plan" =>
            PlanCommand.run(config.pipeline.get)
          case "explain" =>
            ExplainCommand.run(config.pipeline.get)
          case "inspect" =>
            InspectCommand.run(config.pipeline.get, config.inspectId.get)
          case "apply" =>
            ApplyCommand.run(config.pipeline.get, config.env)
          case _ =>
            println("Unknown command. Use --help for usage.")
        }
      case None =>
        sys.exit(1)
    }
  }
}
