package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class PostgreSQLSourceConnector extends SourceConnector {
  def connectorType: String = "PostgreSQL"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val host = config.getOrElse("host", throw new IllegalArgumentException("PostgreSQL: host is required"))
    val port = config.getOrElse("port", "5432")
    val database = config.getOrElse("database", throw new IllegalArgumentException("PostgreSQL: database is required"))
    val user = config.getOrElse("user", throw new IllegalArgumentException("PostgreSQL: user is required"))
    val password = config.getOrElse("password", throw new IllegalArgumentException("PostgreSQL: password is required"))
    val query = config.getOrElse("query", throw new IllegalArgumentException("PostgreSQL: query is required"))
    val url = s"jdbc:postgresql://$host:$port/$database"

    spark.read.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"($query) AS subquery")
      .load()
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    try {
      val host = config.getOrElse("host", return Left("host not configured"))
      val port = config.getOrElse("port", "5432")
      val database = config.getOrElse("database", return Left("database not configured"))
      val user = config.getOrElse("user", return Left("user not configured"))
      val password = config.getOrElse("password", return Left("password not configured"))
      val url = s"jdbc:postgresql://$host:$port/$database"

      val start = System.currentTimeMillis()
      val conn = java.sql.DriverManager.getConnection(url, user, password)
      val latency = System.currentTimeMillis() - start
      conn.close()
      Right(latency)
    } catch {
      case e: Exception => Left(s"Connection failed: ${e.getMessage}")
    }
  }
}
