package com.dataweaver.runner

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
      val confPath = "src/test/resources/test_project/config"
      val runner = new LocalSparkRunner("test", "TestApp")
      runner.run(confPath, None, None)
    } finally {
      spark.stop()
    }
  }
}
