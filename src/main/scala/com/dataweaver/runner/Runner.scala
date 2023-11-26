package com.dataweaver.runner

/** A trait representing a runner for executing Spark jobs.
  */
trait Runner {

  /** Run the Spark job using the specified pipelines and configurations.
    *
    * @param configPath
    *   The project config path
    * @param tag
    *   An optional tag to filter pipeline files.
    * @param regex
    *   An optional regex pattern to filter pipeline files.
    */
  def run(configPath: String, tag: Option[String], regex: Option[String]): Unit
}
