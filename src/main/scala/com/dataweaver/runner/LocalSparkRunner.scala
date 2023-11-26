package com.dataweaver.runner

import com.dataweaver.config.DataWeaverConfig
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import java.io.File

/** A runner for executing Spark jobs locally.
  */
class LocalSparkRunner(executionMode: String, appName: String = "Data Waver App") extends Runner {

  /** Run the Spark job using the specified pipelines and configurations.
    *
    * @param projectConfig
    *   The project config
    * @param tag
    *   An optional tag to filter pipeline files.
    * @param regex
    *   An optional regex pattern to filter pipeline files.
    */
  override def run(
      configPath: String,
      tag: Option[String],
      regex: Option[String]
  ): Unit = {

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
    LocalSparkRunner.executeSparkJob(
      appConfig.getWaverJarPath,
      appConfig.getClusterUrl,
      appName,
      args
    )
  }
}

/** Companion object for RemoteSparkRunner.
  */
object LocalSparkRunner {

  /** Execute the Spark job with the specified JAR using SparkLauncher.
    *
    * @param jarPath
    *   The path to the JAR file.
    * @param clusterUrl
    *   The Spark cluster URL.
    * @param appName
    *   The name of the Spark application.
    */
  private def executeSparkJob(
      jarPath: String,
      clusterUrl: String,
      appName: String,
      args: Seq[String]
  ): Unit = {

    val sparkLauncher = new SparkLauncher()
      .setAppResource(jarPath)
      .setMaster(clusterUrl)
      .setDeployMode("client")
      .setAppName(appName)
      .setMainClass("com.dataweaver.Main")
      .addSparkArg("--packages", "mysql:mysql-connector-java:8.0.33")
      .addAppArgs(args: _*)
      .redirectOutput(new File("/tmp/weaver.log"))
      .redirectError(new File("/tmp/weaver.err.log"))

    sparkLauncher
      .startApplication()
      .addListener(new SparkAppHandle.Listener {
        override def stateChanged(handle: SparkAppHandle): Unit = {
          println(s"Spark job state changed: ${handle.getState}")
        }
        override def infoChanged(handle: SparkAppHandle): Unit = {
          println(s"Spark job info changed: ${handle.getState}")
        }
      })
  }
}
