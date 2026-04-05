package com.dataweaver.core.dag

import com.dataweaver.core.config.{DataSourceConfig, PipelineConfig, TransformationConfig}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGResolverTest extends AnyFlatSpec with Matchers {

  "DAGResolver" should "resolve a linear chain" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("A")),
        TransformationConfig("T2", "SQL", sources = List("T1"))
      )
    )
    val levels = DAGResolver.resolve(config)
    levels should have size 3
    levels(0).map(_.id) should contain only "A"
    levels(1).map(_.id) should contain only "T1"
    levels(2).map(_.id) should contain only "T2"
  }

  it should "detect independent transforms for parallel execution" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test"), DataSourceConfig("B", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("A")),
        TransformationConfig("T2", "SQL", sources = List("B")),
        TransformationConfig("T3", "SQL", sources = List("T1", "T2"))
      )
    )
    val levels = DAGResolver.resolve(config)
    levels should have size 3
    levels(0).map(_.id) should contain allOf ("A", "B")
    levels(1).map(_.id) should contain allOf ("T1", "T2")
    levels(2).map(_.id) should contain only "T3"
  }

  it should "detect cyclic dependencies" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("T2")),
        TransformationConfig("T2", "SQL", sources = List("T1"))
      )
    )
    an[IllegalStateException] should be thrownBy DAGResolver.resolve(config)
  }

  it should "detect missing source references" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("NONEXISTENT"))
      )
    )
    an[IllegalArgumentException] should be thrownBy DAGResolver.resolve(config)
  }
}
