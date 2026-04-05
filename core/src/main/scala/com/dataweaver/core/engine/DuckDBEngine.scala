package com.dataweaver.core.engine

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.{Connection, DriverManager}

object DuckDBEngine extends Engine {
  private val logger = LogManager.getLogger(getClass)
  def name: String = "duckdb"
  private var session: Option[SparkSession] = None
  private var duckConn: Option[Connection] = None

  def getOrCreateSession(appName: String, config: Map[String, String] = Map.empty): SparkSession = {
    session.getOrElse {
      val s = SparkSession.builder()
        .appName(appName).master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
      session = Some(s)
      Class.forName("org.duckdb.DuckDBDriver")
      duckConn = Some(DriverManager.getConnection("jdbc:duckdb:"))
      logger.info("DuckDB engine initialized (in-memory)")
      s
    }
  }

  def sql(query: String)(implicit spark: SparkSession): DataFrame = spark.sql(query)

  def duckQuery(query: String): java.sql.ResultSet = {
    val conn = duckConn.getOrElse(throw new IllegalStateException("DuckDB not initialized"))
    conn.createStatement().executeQuery(query)
  }

  def stop(): Unit = {
    duckConn.foreach(_.close()); duckConn = None
    session.foreach(_.stop()); session = None
  }
}
