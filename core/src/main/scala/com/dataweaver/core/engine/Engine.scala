package com.dataweaver.core.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Engine {
  def name: String
  def getOrCreateSession(appName: String, config: Map[String, String] = Map.empty): SparkSession
  def sql(query: String)(implicit spark: SparkSession): DataFrame
  def stop(): Unit
}
