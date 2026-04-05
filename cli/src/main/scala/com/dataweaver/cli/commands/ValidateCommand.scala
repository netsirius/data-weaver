package com.dataweaver.cli.commands

import com.dataweaver.core.config.{SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.DAGResolver

object ValidateCommand {

  def run(pipelinePath: String): List[String] = {
    YAMLParser.parseFile(pipelinePath) match {
      case Left(parseError) => List(parseError)
      case Right(config) =>
        val schemaErrors = SchemaValidator.validate(config)
        if (schemaErrors.nonEmpty) return schemaErrors

        try {
          DAGResolver.resolve(config)
          println(s"Pipeline '${config.name}' is valid")
          List.empty
        } catch {
          case e: Exception => List(e.getMessage)
        }
    }
  }
}
