package com.dataweaver.core.config

import org.apache.log4j.LogManager

object ProfileApplier {
  private val logger = LogManager.getLogger(getClass)

  def apply(config: PipelineConfig, envName: String): PipelineConfig = {
    config.profiles match {
      case None =>
        logger.info("No profiles defined, using base config")
        config
      case Some(profiles) =>
        profiles.get(envName) match {
          case None =>
            logger.warn(s"Profile '$envName' not found. Available: ${profiles.keys.mkString(", ")}")
            config
          case Some(overrides) =>
            logger.info(s"Applying profile '$envName'")
            applyOverrides(config, overrides)
        }
    }
  }

  private def applyOverrides(config: PipelineConfig, overrides: Map[String, Any]): PipelineConfig = {
    var result = config
    overrides.get("engine").foreach {
      case e: String => result = result.copy(engine = e)
      case _         =>
    }
    result
  }
}
