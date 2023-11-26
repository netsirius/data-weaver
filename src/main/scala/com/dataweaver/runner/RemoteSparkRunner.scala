package com.dataweaver.runner

import com.dataweaver.config.DataWeaverConfig
import org.apache.log4j.LogManager
import org.apache.spark.launcher.SparkLauncher

import scala.sys.process._
import scala.util.{Failure, Success, Try}

/** A runner for executing Spark jobs remotely.
  */
class RemoteSparkRunner(executionMode: String, appName: String = "Data Waver App") extends Runner {
  private val logger = LogManager.getLogger(getClass)

  /** Run the Spark job using the specified pipelines and configurations.
    *
    * @param projectConfig
    *   The project config
    * @param tag
    *   An optional tag to filter pipeline files.
    * @param regex
    *   An optional regex pattern to filter pipeline files.
    */
  override def run(configPath: String, tag: Option[String], regex: Option[String]): Unit = {

    val appConfig = DataWeaverConfig.load(configPath).get
    val filteredPipelineFiles =
      RunnerUtils.getFilteredPipelineFiles(appConfig.getPipelinesDir, tag, regex)
    val configFiles = RunnerUtils.getConfigFiles(configPath)

    // Generate the JAR with pipeline files
    RunnerUtils.addFilesToJar(appConfig.getWaverJarPath, "pipelines", filteredPipelineFiles)

    // Generate the JAR with config files
    RunnerUtils.addFilesToJar(appConfig.getWaverJarPath, "pipelines_config", configFiles)

    // Prepare app args
    val tagArg: Seq[String] = tag.map(value => Seq("--tag", value)).getOrElse(Seq.empty[String])
    val regexArg: Seq[String] =
      regex.map(value => Seq("--regex", value)).getOrElse(Seq.empty[String])
    val executionModeArg: Seq[String] = Seq("--executionMode", executionMode)

    val args: Seq[String] = tagArg ++ regexArg ++ executionModeArg

    // Execute the Spark job
    RemoteSparkRunner.executeSparkJob(
      appConfig.getWaverJarPath,
      appConfig.getClusterUrl,
      appName,
      args
    )
  }
}

/** Companion object for RemoteSparkRunner.
  */
object RemoteSparkRunner {

  /** Execute the spark-submit command with the specified JAR.
    *
    * @param jarPath
    *   The path to the JAR file.
    * @param clusterUrl
    *   The Spark cluster URL.
    * @param appName
    *   The name of the Spark application.
    */
  def executeSparkSubmit(jarPath: String, clusterUrl: String, appName: String): Unit = {
    val sparkSubmitCommand =
      s"spark-submit --master $clusterUrl --class my.package.MainClass $jarPath"

    Try(sparkSubmitCommand.! /* Execute the command and capture the result */ ) match {
      case Success(exitCode) if exitCode == 0 =>
        // Success: The command executed without errors
        println("Spark job completed successfully.")
      case Success(exitCode) =>
        // The command finished with a non-zero exit code (indicating an error)
        println(s"Spark job failed with exit code: $exitCode")
      // You can add additional error handling here if necessary
      case Failure(exception) =>
        // An exception occurred while executing the command
        println(s"Error executing spark-submit: ${exception.getMessage}")
      // You can add additional error handling here if necessary
    }
  }

  /** Execute the Spark job with the specified JAR using SparkLauncher.
    *
    * @param jarPath
    *   The path to the JAR file.
    * @param clusterUrl
    *   The Spark cluster URL.
    * @param appName
    *   The name of the Spark application.
    */
  def executeSparkJob(
      jarPath: String,
      clusterUrl: String,
      appName: String,
      args: Seq[String]
  ): Unit = {

    val sparkLauncher = new SparkLauncher()
      .setAppResource(jarPath)
      .setMaster(clusterUrl)
      .setAppName(appName)
      .setMainClass("com.dataweaver.Main")
      .addSparkArg("--packages", "mysql:mysql-connector-java:8.0.33")
      .addAppArgs(args: _*)

    Try(sparkLauncher.launch()) match {
      case Success(process) =>
        // Wait for the Spark job to finish
        val exitCode = process.waitFor()
        if (exitCode == 0) {
          // Success: The Spark job completed without errors
          println("Spark job completed successfully.")
        } else {
          // The Spark job finished with a non-zero exit code (indicating an error)
          println(s"Spark job failed with exit code: $exitCode")
          // You can add additional error handling here if necessary
        }

      case Failure(exception) =>
        // An exception occurred while launching the Spark job
        println(s"Error launching Spark job: ${exception.getMessage}")
      // You can add additional error handling here if necessary
    }
  }
}
