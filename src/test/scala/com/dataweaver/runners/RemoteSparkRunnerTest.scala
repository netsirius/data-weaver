package com.dataweaver.runners

import com.dataweaver.parsing.YAMLParser
import com.dataweaver.sinks.BigQuerySink
import com.dataweaver.sources.MySQLSource
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class RemoteSparkRunnerTest extends AnyFlatSpec with Matchers {
  "DataPipeline" should "execute with mock data" in {
    implicit val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("DataPipelineTest")
      .getOrCreate()

    try {
      // Cargar el archivo YAML
      val pipelineConfig = YAMLParser.parse("/path/to/pipeline.yaml")

      // Crear y configurar los managers de prueba
      val dataSourceManager = new TestDataSourceManager()
      val dataSinkManager = new TestDataSinkManager()

      // Reemplazar los managers reales con los de prueba en tu pipeline
      val dataSource = new MySQLSource(pipelineConfig.head.dataSources.head, dataSourceManager)
      val dataSink = new BigQuerySink(pipelineConfig.head.sinks.head, dataSinkManager)

      // Ejecutar el pipeline
      val data = dataSource.readData()
      dataSink.writeData(data)
    } finally {
      spark.stop()
    }
  }
}