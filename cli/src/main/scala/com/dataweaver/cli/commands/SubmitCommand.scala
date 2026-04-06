package com.dataweaver.cli.commands

import org.apache.log4j.LogManager

import scala.sys.process._
import scala.util.{Failure, Success, Try}

/** Submits a Data Weaver pipeline to a remote Spark cluster.
  * Wraps spark-submit for various deployment targets.
  *
  * Targets: standalone, k8s, emr, dataproc
  */
object SubmitCommand {
  private val logger = LogManager.getLogger(getClass)

  def run(
      pipelinePath: String,
      target: String,
      env: Option[String] = None,
      config: Map[String, String] = Map.empty
  ): Unit = {
    val jarPath = config.getOrElse("jar", findJar())
    val envFlag = env.map(e => s"--env $e").getOrElse("")

    target match {
      case "standalone" => submitStandalone(jarPath, pipelinePath, envFlag, config)
      case "k8s"        => submitK8s(jarPath, pipelinePath, envFlag, config)
      case "emr"        => submitEMR(jarPath, pipelinePath, envFlag, config)
      case "dataproc"   => submitDataproc(jarPath, pipelinePath, envFlag, config)
      case other        => System.err.println(s"Unknown submit target: $other. Use: standalone, k8s, emr, dataproc")
    }
  }

  private def submitStandalone(
      jar: String, pipeline: String, envFlag: String, config: Map[String, String]
  ): Unit = {
    val master = config.getOrElse("master", "spark://localhost:7077")
    val cmd = s"""spark-submit --master $master --class com.dataweaver.cli.WeaverCLI $jar apply $pipeline $envFlag"""
    executeCommand(cmd)
  }

  private def submitK8s(
      jar: String, pipeline: String, envFlag: String, config: Map[String, String]
  ): Unit = {
    val master = config.getOrElse("master", throw new IllegalArgumentException("k8s requires --master"))
    val image = config.getOrElse("image", "data-weaver:latest")
    val namespace = config.getOrElse("namespace", "default")

    val cmd = s"""spark-submit
      --master $master
      --deploy-mode cluster
      --conf spark.kubernetes.container.image=$image
      --conf spark.kubernetes.namespace=$namespace
      --class com.dataweaver.cli.WeaverCLI
      local:///app/data-weaver.jar apply $pipeline $envFlag""".replaceAll("\\s+", " ").trim

    executeCommand(cmd)
  }

  private def submitEMR(
      jar: String, pipeline: String, envFlag: String, config: Map[String, String]
  ): Unit = {
    val clusterId = config.getOrElse("cluster-id",
      throw new IllegalArgumentException("EMR requires --cluster-id"))

    val cmd = s"""aws emr add-steps
      --cluster-id $clusterId
      --steps Type=Spark,Name=DataWeaver,Args=[--class,com.dataweaver.cli.WeaverCLI,$jar,apply,$pipeline${if (envFlag.nonEmpty) s",$envFlag" else ""}],ActionOnFailure=CONTINUE""".replaceAll("\\s+", " ").trim

    executeCommand(cmd)
  }

  private def submitDataproc(
      jar: String, pipeline: String, envFlag: String, config: Map[String, String]
  ): Unit = {
    val cluster = config.getOrElse("cluster",
      throw new IllegalArgumentException("Dataproc requires --cluster"))
    val region = config.getOrElse("region", "us-central1")

    val cmd = s"""gcloud dataproc jobs submit spark
      --cluster=$cluster --region=$region
      --class=com.dataweaver.cli.WeaverCLI
      --jars=$jar
      -- apply $pipeline $envFlag""".replaceAll("\\s+", " ").trim

    executeCommand(cmd)
  }

  private def executeCommand(cmd: String): Unit = {
    println(s"Executing: $cmd")
    Try(cmd.!) match {
      case Success(0)    => println("Submission completed successfully.")
      case Success(code) => System.err.println(s"Submission failed with exit code: $code")
      case Failure(e)    => System.err.println(s"Submission error: ${e.getMessage}")
    }
  }

  private def findJar(): String = {
    val candidates = List(
      "cli/target/scala-2.13/data-weaver.jar",
      "data-weaver.jar",
      sys.env.getOrElse("WEAVER_JAR", "")
    ).filter(_.nonEmpty)

    candidates.find(p => new java.io.File(p).exists())
      .getOrElse(throw new IllegalArgumentException(
        "Cannot find data-weaver.jar. Build it with: sbt 'cli/assembly'"))
  }
}
