package com.dataweaver.reader

import com.dataweaver.config.DataSourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * A DataSource implementation for reading data from a MySQL database.
 *
 * @param config The configuration for the MySQL data source.
 */

class SQLReader(config: DataSourceConfig) extends DataReader {

  /**
   * The name of the data source.
   *
   * @return The name of the data source.
   */
  def sourceName: String = config.id

  /**
   * Reads data from the MySQL database using the JDBC connection string and table name from the configuration.
   *
   * @param spark The SparkSession for executing the read operation.
   * @return A DataFrame containing the data from the MySQL database.
   */
  override def readData()(implicit spark: SparkSession): DataFrame = {
    val host = config.config.getOrElse("host", throw new IllegalArgumentException("Connection host is required"))
    val db = config.config.getOrElse("db", throw new IllegalArgumentException("Connection db is required"))
    val query = config.config.getOrElse("query", throw new IllegalArgumentException("Query is required"))
    val url = s"jdbc:sqlserver://$host;databaseName=$db;"

    val user = config.config.getOrElse("user", throw new IllegalArgumentException("User name is required"))
    val password = config.config.getOrElse("password", throw new IllegalArgumentException("Password is required"))

    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", query)
      .load()
  }

}

