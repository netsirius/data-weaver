package com.dataweaver.cli

import com.dataweaver.cli.commands.{ApplyCommand, DoctorCommand, ValidateCommand}
import org.apache.spark.sql.SparkSession
import scopt.OParser

object WeaverCLI {

  case class Config(
      command: String = "",
      pipeline: Option[String] = None,
      env: Option[String] = None
  )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("weaver"),
        head("Data Weaver", "0.2.0"),
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
          case "doctor" =>
            val result = DoctorCommand.run(config.pipeline.get)
            if (!result.overallHealthy) sys.exit(1)
          case "validate" =>
            val errors = ValidateCommand.run(config.pipeline.get)
            if (errors.nonEmpty) {
              errors.foreach(e => System.err.println(s"ERROR: $e"))
              sys.exit(1)
            }
          case "apply" =>
            implicit val spark: SparkSession = SparkSession.builder()
              .appName("DataWeaver")
              .getOrCreate()
            try {
              ApplyCommand.run(config.pipeline.get)
            } finally {
              spark.stop()
            }
          case _ =>
            println("Unknown command. Use --help for usage.")
        }
      case None =>
        sys.exit(1)
    }
  }
}
