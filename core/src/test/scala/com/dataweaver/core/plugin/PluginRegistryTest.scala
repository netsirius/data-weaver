package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PluginRegistryTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = PluginRegistry.reset()

  "PluginRegistry" should "register and retrieve a source connector" in {
    val mockSource = new SourceConnector {
      def connectorType: String = "MockDB"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame =
        throw new UnsupportedOperationException
    }
    PluginRegistry.registerSource(mockSource)
    PluginRegistry.getSource("MockDB") shouldBe Some(mockSource)
    PluginRegistry.getSource("Unknown") shouldBe None
  }

  it should "register and retrieve a sink connector" in {
    val mockSink = new SinkConnector {
      def connectorType: String = "MockSink"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = ()
    }
    PluginRegistry.registerSink(mockSink)
    PluginRegistry.getSink("MockSink") shouldBe Some(mockSink)
  }

  it should "register and retrieve a transform plugin" in {
    val mockTransform = new TransformPlugin {
      def transformType: String = "MockTransform"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = throw new UnsupportedOperationException
    }
    PluginRegistry.registerTransform(mockTransform)
    PluginRegistry.getTransform("MockTransform") shouldBe Some(mockTransform)
  }

  it should "list available connectors" in {
    PluginRegistry.availableSources shouldBe empty
    val mockSource = new SourceConnector {
      def connectorType: String = "TestDB"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame =
        throw new UnsupportedOperationException
    }
    PluginRegistry.registerSource(mockSource)
    PluginRegistry.availableSources should contain("TestDB")
  }
}
