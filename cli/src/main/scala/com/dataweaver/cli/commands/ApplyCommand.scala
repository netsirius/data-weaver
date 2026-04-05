package com.dataweaver.cli.commands

import com.dataweaver.core.config.YAMLParser
import com.dataweaver.core.engine.PipelineExecutor
import org.apache.spark.sql.SparkSession

object ApplyCommand {

  def run(pipelinePath: String)(implicit spark: SparkSession): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        throw new IllegalArgumentException(s"Failed to parse pipeline: $err")
    }

    PipelineExecutor.execute(config)
  }
}
