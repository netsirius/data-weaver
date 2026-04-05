package com.dataweaver.core.engine

import com.dataweaver.core.config.PipelineConfig
import org.apache.log4j.LogManager

object EngineSelector {
  private val logger = LogManager.getLogger(getClass)
  val DEFAULT_THRESHOLD_BYTES: Long = 1L * 1024 * 1024 * 1024

  def select(config: PipelineConfig): Engine = {
    config.engine.toLowerCase match {
      case "spark" =>
        logger.info("Engine: Spark (explicit)")
        SparkEngine
      case "local" =>
        logger.info("Engine: DuckDB (explicit local)")
        DuckDBEngine
      case "auto" =>
        logger.info("Engine: DuckDB (auto — local mode)")
        DuckDBEngine
      case other =>
        throw new IllegalArgumentException(s"Unknown engine '$other'. Valid: spark, local, auto")
    }
  }
}
