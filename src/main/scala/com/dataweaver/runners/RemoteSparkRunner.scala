package com.dataweaver.runners

import org.apache.spark.launcher.SparkLauncher

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.jar.{JarEntry, JarOutputStream}
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
 * A runner for executing Spark jobs remotely.
 */
class RemoteSparkRunner(jarPath: String, clusterUrl: String, appName: String) extends Runner {
  /**
   * Run the Spark job using the specified pipelines and configurations.
   *
   * @param pipelinesFolder The folder containing pipeline files.
   * @param tag             An optional tag to filter pipeline files.
   * @param regex           An optional regex pattern to filter pipeline files.
   */
  override def run(pipelinesFolder: String, tag: Option[String], regex: Option[String]): Unit = {
    // Get and filter pipeline files
    val filteredFiles = RemoteSparkRunner.getFilteredPipelineFiles(pipelinesFolder, tag, regex)

    // Generate the JAR with pipeline files
    RemoteSparkRunner.createJarWithPipelines(filteredFiles)

    // Execute spark-submit
    RemoteSparkRunner.executeSparkJob(jarPath, clusterUrl, appName, tag.getOrElse(""), regex.getOrElse(""))
  }
}

/**
 * Companion object for RemoteSparkRunner.
 */
object RemoteSparkRunner {
  // Create a temporary directory for JAR files
  private val jarDirectory = Files.createTempDirectory("weaver_jars")
  private val jarDirectoryPath = jarDirectory.toString

  /**
   * Get filtered pipeline files from the specified folder based on tag and regex patterns.
   *
   * @param pipelinesFolder The folder containing pipeline files.
   * @param tag             An optional tag to filter pipeline files.
   * @param regex           An optional regex pattern to filter pipeline files.
   * @return A sequence of filtered pipeline files.
   */
  def getFilteredPipelineFiles(pipelinesFolder: String, tag: Option[String], regex: Option[String]): Seq[File] = {
    val pipelineDirectory = new File(pipelinesFolder)
    val allFiles = pipelineDirectory.listFiles()

    // Filter by tag if provided
    val taggedFiles = tag match {
      case Some(tagValue) => allFiles.filter(file => file.getName.contains(tagValue))
      case None => allFiles
    }

    // Filter by regex if provided
    val filteredFiles = regex match {
      case Some(regexPattern) => taggedFiles.filter(file => file.getName.matches(regexPattern))
      case None => taggedFiles
    }

    filteredFiles
  }

  /**
   * Create a JAR file containing the specified files.
   *
   * @param files The sequence of files to include in the JAR.
   * @return The path to the generated JAR file.
   */
  def createJarWithPipelines(files: Seq[File]): String = {
    val jarFileName = "pipelines.jar"
    val jarFilePath = Paths.get(jarDirectoryPath, jarFileName).toString

    val jarStream = new JarOutputStream(new FileOutputStream(jarFilePath))

    files.foreach { file =>
      val entryName = s"pipelines/${file.getName}"
      val entry = new JarEntry(entryName)
      jarStream.putNextEntry(entry)

      val fileStream = new FileInputStream(file)
      val buffer = new Array[Byte](1024)
      var bytesRead = fileStream.read(buffer)

      while (bytesRead != -1) {
        jarStream.write(buffer, 0, bytesRead)
        bytesRead = fileStream.read(buffer)
      }

      fileStream.close()
      jarStream.closeEntry()
    }

    jarStream.close()

    jarFilePath
  }

  /**
   * Execute the spark-submit command with the specified JAR.
   *
   * @param jarPath    The path to the JAR file.
   * @param clusterUrl The Spark cluster URL.
   * @param appName    The name of the Spark application.
   */
  def executeSparkSubmit(jarPath: String, clusterUrl: String, appName: String): Unit = {
    val sparkSubmitCommand = s"spark-submit --master $clusterUrl --class my.package.MainClass $jarPath"

    Try(sparkSubmitCommand.! /* Execute the command and capture the result */) match {
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

  /**
   * Execute the Spark job with the specified JAR using SparkLauncher.
   *
   * @param jarPath    The path to the JAR file.
   * @param clusterUrl The Spark cluster URL.
   * @param appName    The name of the Spark application.
   */
  def executeSparkJob(jarPath: String, clusterUrl: String, appName: String, tag: String, regex: String): Unit = {
    val sparkLauncher = new SparkLauncher()
      .setAppResource(jarPath)
      .setMaster(clusterUrl)
      .setAppName(appName)
      .setMainClass("com.dataweaver.Main")
      .addAppArgs(tag, regex)

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