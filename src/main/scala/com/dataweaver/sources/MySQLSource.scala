package com.dataweaver.sources

import com.dataweaver.config.DataSourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * A DataSource implementation for reading data from a MySQL database.
 *
 * @param config The configuration for the MySQL data source.
 */
class MySQLSource(config: DataSourceConfig) extends DataSource {

  /**
   * The name of the data source.
   *
   * @return The name of the data source.
   */
  def name: String = config.id

  /**
   * Reads data from the MySQL database using the JDBC connection string and table name from the configuration.
   *
   * @param spark The SparkSession for executing the read operation.
   * @return A DataFrame containing the data from the MySQL database.
   */
  override def readData()(implicit spark: SparkSession): DataFrame = {
    val connectionString = config.config.getOrElse("connectionString", throw new IllegalArgumentException("Connection string is required"))
    val tableName = config.config.getOrElse("tableName", throw new IllegalArgumentException("Table name is required"))

    spark.read
      .format("jdbc")
      .option("url", connectionString)
      .option("dbtable", tableName)
      .load()
  }

}

