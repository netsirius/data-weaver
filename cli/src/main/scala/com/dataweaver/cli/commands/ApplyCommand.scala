package com.dataweaver.cli.commands

import com.dataweaver.core.config.{ProfileApplier, YAMLParser}
import com.dataweaver.core.engine.{EngineSelector, PipelineExecutor}

object ApplyCommand {

  def run(pipelinePath: String, env: Option[String] = None): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        throw new IllegalArgumentException(s"Failed to parse pipeline: $err")
    }

    // Apply profile overrides if --env specified
    val resolvedConfig = env.map(ProfileApplier.apply(config, _)).getOrElse(config)

    // Select engine based on config
    val engine = EngineSelector.select(resolvedConfig)
    implicit val spark = engine.getOrCreateSession("DataWeaver")

    try {
      PipelineExecutor.execute(resolvedConfig)
    } finally {
      engine.stop()
    }
  }
}
