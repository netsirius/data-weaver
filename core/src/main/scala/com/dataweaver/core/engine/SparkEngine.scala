package com.dataweaver.core.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkEngine extends Engine {
  def name: String = "spark"
  private var session: Option[SparkSession] = None

  def getOrCreateSession(appName: String, config: Map[String, String] = Map.empty): SparkSession = {
    session.getOrElse {
      val builder = SparkSession.builder().appName(appName)
      config.foreach { case (k, v) => if (k.startsWith("spark.")) builder.config(k, v) }
      if (!config.contains("spark.master")) builder.master("local[*]")
      val s = builder.getOrCreate()
      session = Some(s)
      s
    }
  }

  def sql(query: String)(implicit spark: SparkSession): DataFrame = spark.sql(query)

  def stop(): Unit = { session.foreach(_.stop()); session = None }
}
