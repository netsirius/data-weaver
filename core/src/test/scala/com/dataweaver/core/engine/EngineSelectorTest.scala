package com.dataweaver.core.engine

import com.dataweaver.core.config.PipelineConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineSelectorTest extends AnyFlatSpec with Matchers {

  "EngineSelector" should "select Spark when engine is 'spark'" in {
    val config = PipelineConfig(name = "test", engine = "spark")
    EngineSelector.select(config) shouldBe SparkEngine
  }

  it should "select DuckDB when engine is 'local'" in {
    val config = PipelineConfig(name = "test", engine = "local")
    EngineSelector.select(config) shouldBe DuckDBEngine
  }

  it should "select DuckDB for auto mode" in {
    val config = PipelineConfig(name = "test", engine = "auto")
    EngineSelector.select(config) shouldBe DuckDBEngine
  }

  it should "throw on unknown engine" in {
    val config = PipelineConfig(name = "test", engine = "quantum")
    an[IllegalArgumentException] should be thrownBy EngineSelector.select(config)
  }
}
