package com.dataweaver

import com.dataweaver.config.DataWeaverConfig
import com.dataweaver.core.DataFlowExecutor
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    val tag = if (args.nonEmpty) Some(args(0)) else None
    val regex = if (args.nonEmpty) Some(args(1)) else None

    implicit val spark = SparkSession.builder
      .getOrCreate()

    try {
      println(s"Running DataWeaver with tag: $tag and regex: $regex")
      val configPath = "./config"
      DataWeaverConfig.load(configPath) match {
        case Success(projectConfig) =>
          val manager = new DataFlowExecutor(projectConfig)
          manager.executeDataFlows(tag, regex)
        case Failure(ex) =>
          println(s"Error loading configuration: ${ex.getMessage}")
      }
    } finally {
      spark.stop()
    }
  }
}
