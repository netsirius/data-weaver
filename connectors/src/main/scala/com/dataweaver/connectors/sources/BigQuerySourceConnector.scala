package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Reads data from BigQuery tables or queries.
  *
  * Config:
  *   projectId  - GCP project ID
  *   dataset    - BigQuery dataset name
  *   table      - Table name (for direct table reads)
  *   query      - SQL query (alternative to table, uses BigQuery SQL)
  *   temporaryGcsBucket - GCS bucket for temporary data (required)
  */
class BigQuerySourceConnector extends SourceConnector {
  def connectorType: String = "BigQuery"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val temporaryGcsBucket = config.getOrElse("temporaryGcsBucket",
      throw new IllegalArgumentException("BigQuery source: 'temporaryGcsBucket' is required"))

    val query = config.get("query")
    val table = for {
      project <- config.get("projectId")
      dataset <- config.get("dataset")
      tbl     <- config.get("table")
    } yield s"$project.$dataset.$tbl"

    val reader = spark.read
      .format("bigquery")
      .option("temporaryGcsBucket", temporaryGcsBucket)

    (query, table) match {
      case (Some(q), _) =>
        reader.option("query", q).load()
      case (_, Some(t)) =>
        reader.option("table", t).load()
      case _ =>
        throw new IllegalArgumentException(
          "BigQuery source requires either 'query' or 'projectId'+'dataset'+'table'")
    }
  }
}
