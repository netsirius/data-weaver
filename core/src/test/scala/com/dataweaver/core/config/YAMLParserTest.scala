package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YAMLParserTest extends AnyFlatSpec with Matchers {

  "YAMLParser" should "parse a valid pipeline YAML" in {
    val result = YAMLParser.parseFile("core/src/test/resources/test_project/pipelines/pipeline.yaml")
    result.isRight shouldBe true

    val config = result.toOption.get
    config.name shouldBe "ExamplePipeline"
    config.tag shouldBe "example"
    config.engine shouldBe "auto"
    config.dataSources should have size 1
    config.transformations should have size 2
    config.sinks should have size 1
  }

  it should "parse sink with explicit source routing" in {
    val result = YAMLParser.parseFile("core/src/test/resources/test_project/pipelines/pipeline.yaml")
    val config = result.toOption.get
    config.sinks.head.source shouldBe Some("transform2")
  }

  it should "return error for non-existent file" in {
    val result = YAMLParser.parseFile("nonexistent.yaml")
    result.isLeft shouldBe true
  }

  it should "return error for invalid YAML" in {
    val result = YAMLParser.parseString("not: [valid: yaml: {{")
    result.isLeft shouldBe true
  }
}
