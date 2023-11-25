package com.dataweaver.parsing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YAMLParserTest extends AnyFlatSpec with Matchers {

  /**
   * Test for the YAMLParser class.
   */
  "YAMLParser" should "correctly parse a YAML file into a DataPipelineConfig object with expected properties" in {
    val testPipelinePath = "src/test/resources/test_project/pipelines/pipeline.yaml"

    val parsedConfig = YAMLParser.parse(testPipelinePath)
    parsedConfig should not be empty
    val config = parsedConfig.head

    // Verify general properties of DataPipelineConfig
    config.name shouldBe "ExamplePipeline"
    config.tag shouldBe "example"

    // Verify dataSources
    config.dataSources should have size 1
    val dataSource = config.dataSources.head
    dataSource.id shouldBe "testSource"
    dataSource.`type` shouldBe "MySQL"
    dataSource.query.trim shouldBe "SELECT name FROM test_table"

    // Verify transformations
    config.transformations should have size 2
    val transform1 = config.transformations.head
    transform1.id shouldBe "transform1"
    transform1.`type` shouldBe "SQLTransformation"
    transform1.sources should contain only "source1"
    transform1.query.get.trim shouldBe "SELECT * FROM testSource WHERE column1 = 'value'"

    val transform2 = config.transformations(1)
    transform2.id shouldBe "transform2"
    transform2.`type` shouldBe "ScalaTransformation"
    transform2.sources should contain only "transform1"
    transform2.action.get shouldBe "dropDuplicates"

    // Verify sinks
    config.sinks should have size 1
    val sink = config.sinks.head
    sink.id shouldBe "sink1"
    sink.`type` shouldBe "BigQuery"
    sink.config("saveMode") shouldBe "Append"
    sink.config("profile") shouldBe "testProfile"
  }
}