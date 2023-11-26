package com.dataweaver

import com.dataweaver.config.{ArgsParser, DataWeaverConfig, ExecutionMode}
import com.dataweaver.core.DataFlowExecutor
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

object Main {
  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    ArgsParser.parse(args) match {
      case Some(args_config) =>
        // config contiene tag, regex y executionMode
        implicit val spark: SparkSession = {
          val conf = config.SPARK_CONF.clone()
          // conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credential)

          var builder = SparkSession
            .builder()
            .config(conf)

          builder.getOrCreate()
        }
        // spark.sparkContext.setLogLevel("WARN")

        try {
          logger.info(
            s"Running DataWeaver with execution mode: ${args_config.executionMode}, tag: ${args_config.tag} and regex: ${args_config.regex}"
          )

          val configPath = args_config.configPath.getOrElse("/pipelines_config")
          DataWeaverConfig.load(configPath) match {
            case Success(projectConfig) =>
              logger.info(s"Loaded project config: $projectConfig")
              val manager = new DataFlowExecutor(projectConfig)
              manager.executeDataFlows(
                args_config.tag,
                args_config.regex,
                ExecutionMode.fromString(args_config.executionMode.get)
              )
            case Failure(ex) =>
              println(s"Error loading configuration: ${ex.getMessage}")
          }
        } finally {
          spark.stop()
        }

      case None =>
        // El análisis de argumentos falló. Manejar según sea necesario.
        println("Error en los argumentos proporcionados.")
    }
  }
}
