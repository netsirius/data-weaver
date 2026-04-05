package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PipelineConfigTest extends AnyFlatSpec with Matchers {

  "PipelineConfig" should "be constructable with defaults" in {
    val config = PipelineConfig(name = "test")
    config.engine shouldBe "auto"
    config.dataSources shouldBe empty
    config.transformations shouldBe empty
    config.sinks shouldBe empty
  }

  "SinkConfig" should "have explicit source routing" in {
    val sink = SinkConfig(id = "warehouse", `type` = "BigQuery", source = Some("validated"))
    sink.source shouldBe Some("validated")
  }

  it should "default source to None" in {
    val sink = SinkConfig(id = "out", `type` = "Test")
    sink.source shouldBe None
  }

  "ExecutionMode" should "parse valid modes" in {
    ExecutionMode.fromString("test") shouldBe ExecutionMode.Test
    ExecutionMode.fromString("production") shouldBe ExecutionMode.Production
    ExecutionMode.fromString("debug") shouldBe ExecutionMode.Debug
  }

  it should "throw on invalid mode" in {
    an[IllegalArgumentException] should be thrownBy ExecutionMode.fromString("invalid")
  }
}
