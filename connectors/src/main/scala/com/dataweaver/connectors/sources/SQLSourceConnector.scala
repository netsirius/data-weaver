package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class SQLSourceConnector extends SourceConnector {
  def connectorType: String = "MySQL"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val host = config.getOrElse("host", throw new IllegalArgumentException("host is required"))
    val port = config.getOrElse("port", "3306")
    val db = config.getOrElse("db", throw new IllegalArgumentException("db is required"))
    val query = config.getOrElse("query", throw new IllegalArgumentException("query is required"))
    val driver = config.getOrElse("driver", "com.mysql.cj.jdbc.Driver")
    val url = if (driver.contains("sqlserver")) s"jdbc:sqlserver://$host;databaseName=$db;"
              else s"jdbc:mysql://$host:$port/$db"
    val user = config.getOrElse("user", throw new IllegalArgumentException("user is required"))
    val password = config.getOrElse("password", throw new IllegalArgumentException("password is required"))

    spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", query)
      .load()
  }
}
