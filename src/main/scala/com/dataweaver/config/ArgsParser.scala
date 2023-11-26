package com.dataweaver.config

import scopt.OParser

object ArgsParser {
  case class Config(
      tag: Option[String] = None,
      regex: Option[String] = None,
      executionMode: Option[String] = None,
      configPath: Option[String] = None
  )

  val parser: OParser[Unit, Config] = {
    val builder = OParser.builder[Config]
    import builder._

    OParser.sequence(
      programName("DataWeaver"),
      opt[String]("tag")
        .optional()
        .action((x, c) => c.copy(tag = Some(x)))
        .text("Tag to use"),
      opt[String]("regex")
        .optional()
        .action((x, c) => c.copy(regex = Some(x)))
        .text("Regex pattern"),
      opt[String]("executionMode")
        .optional()
        .action((x, c) => c.copy(executionMode = Some(x)))
        .text("Execution mode"),
      opt[String]("configPath")
        .optional()
        .action((x, c) => c.copy(configPath = Some(x)))
        .text("Config path")
    )
  }

  def parse(args: Array[String]): Option[Config] = {
    OParser.parse(parser, args, Config())
  }
}
