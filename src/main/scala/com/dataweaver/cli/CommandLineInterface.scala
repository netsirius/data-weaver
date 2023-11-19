package com.dataweaver.cli

import com.dataweaver.config.AppConfig
import com.dataweaver.runners.{LocalSparkRunner, RemoteSparkRunner}
import scopt.OParser

case class Config(
                   command: String = "",
                   regex: Option[String] = None,
                   tag: Option[String] = None,
                   reload: Boolean = false,
                   show: Boolean = false
                 )

object CommandLineInterface {

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("weaver"),
        head("weaver", "1.0"),

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

        cmd("config")
          .action((_, c) => c.copy(command = "config"))
          .text("System configuration.")
          .children(
            opt[Unit]("reload")
              .optional()
              .action((_, c) => c.copy(reload = true))
              .text("Reload configuration."),
            opt[Unit]("show")
              .optional()
              .action((_, c) => c.copy(show = true))
              .text("Show current configuration.")
          )
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.command match {
          case "run" =>
            val pipelinesFolder = AppConfig.loadConfig.get.getString("pipelines.folder")
            val runnerType = AppConfig.loadConfig.get.getString("runner.type")
            val runner = runnerType match {
              case "local" => new LocalSparkRunner
              case "remote" => new RemoteSparkRunner
              case _ => throw new IllegalArgumentException("Unspecified or incorrect runner type")
            }
            runner.run(config.tag, config.regex)
          case "config" =>
            if (config.reload) {
              // Logic to reload configuration
            }
            if (config.show) {
              // Logic to show current configuration
            }
        }
      case _ =>
      // Invalid arguments provided
    }
  }
}
