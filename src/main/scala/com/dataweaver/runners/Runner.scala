package com.dataweaver.runners

import com.dataweaver.config.DataWeaverConfig

/**
 * A trait representing a runner for executing Spark jobs.
 */
trait Runner {
  /**
   * Run the Spark job using the specified pipelines and configurations.
   *
   * @param projectConfig The project config
   * @param tag           An optional tag to filter pipeline files.
   * @param regex         An optional regex pattern to filter pipeline files.
   */
  def run(projectConfig: DataWeaverConfig, tag: Option[String], regex: Option[String]): Unit
}