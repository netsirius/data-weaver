package com.dataweaver.runners

import com.dataweaver.parsing.YAMLParser
import org.apache.spark.sql.SparkSession
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class RemoteSparkRunnerTest extends AnyFlatSpec with Matchers with MockitoSugar {
  "DataPipeline" should "execute with mock data" in {
    implicit val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("DataPipelineTest")
      .getOrCreate()

    try {
      // Cargar el archivo YAML
      val pipelineConfig = YAMLParser.parse("/path/to/pipeline.yaml")
      //
      //      // Crear un esquema para el DataFrame
      //      val schema = StructType(Array(StructField("column1", StringType, true)))
      //      // Crear datos para el DataFrame
      //      val rowData = Seq(Row("data1"), Row("data2"))
      //      // Crear un DataFrame de mock
      //      val mockDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rowData), schema)
      //
      //      // Mockear el método read de SparkSession
      //      val mockedDataFrameReader = mock[DataFrameReader]
      //      when(mockedDataFrameReader.).thenReturn(mockedDataFrameReader)
      //      val mockedDataFrameReader = mock[spark.read]
      //
      //      // Crear y configurar los managers de prueba
      //      val dataSourceManager = new TestDataSourceManager()
      //      val dataSinkManager = new TestDataSinkManager()
      //
      //      // Reemplazar los managers reales con los de prueba en tu pipeline
      //      val dataSource = new SQLReader(pipelineConfig.head.dataSources.head, dataSourceManager)
      //      val dataSink = new BigQuerySink(pipelineConfig.head.sinks.head, dataSinkManager)
      //
      //      // Ejecutar el pipeline
      //      val data = dataSource.readData()
      //      dataSink.writeData(data)
    } finally {
      spark.stop()
    }
  }
}