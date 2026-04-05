package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaValidatorTest extends AnyFlatSpec with Matchers {

  "SchemaValidator" should "pass a valid pipeline" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("src1", "Test")),
      transformations = List(TransformationConfig("t1", "SQL", sources = List("src1"))),
      sinks = List(SinkConfig("s1", "Test", source = Some("t1")))
    )
    val errors = SchemaValidator.validate(config)
    errors shouldBe empty
  }

  it should "report missing pipeline name" in {
    val config = PipelineConfig(name = "")
    val errors = SchemaValidator.validate(config)
    errors should contain("Pipeline name cannot be empty")
  }

  it should "report sink referencing non-existent transform" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("src1", "Test")),
      sinks = List(SinkConfig("s1", "Test", source = Some("NONEXISTENT")))
    )
    val errors = SchemaValidator.validate(config)
    errors.exists(_.contains("NONEXISTENT")) shouldBe true
  }

  it should "report duplicate ids" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("dup", "Test"), DataSourceConfig("dup", "Test"))
    )
    val errors = SchemaValidator.validate(config)
    errors.exists(_.contains("Duplicate")) shouldBe true
  }

  it should "report sink without source when transforms exist" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("src1", "Test")),
      transformations = List(TransformationConfig("t1", "SQL", sources = List("src1"))),
      sinks = List(SinkConfig("s1", "Test", source = None))
    )
    val errors = SchemaValidator.validate(config)
    errors.exists(_.contains("source")) shouldBe true
  }
}
