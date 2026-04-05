package com.dataweaver.core.engine

import com.dataweaver.core.config._
import com.dataweaver.core.plugin._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PipelineExecutorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("ExecutorTest").getOrCreate()
    PluginRegistry.reset()
  }

  override def afterAll(): Unit = spark.stop()

  "PipelineExecutor" should "execute a pipeline with explicit sink routing" in {
    PluginRegistry.reset()

    PluginRegistry.registerSource(new SourceConnector {
      def connectorType = "InMemory"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq(("Alice", 1), ("Bob", 2), ("Charlie", 3)).toDF("name", "id")
      }
    })

    PluginRegistry.registerTransform(new TransformPlugin {
      def transformType = "SQL"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = {
        config.sources.foreach { srcId =>
          inputs(srcId).createOrReplaceTempView(srcId)
        }
        spark.sql(config.query.getOrElse("SELECT * FROM " + config.sources.head))
      }
    })

    var capturedData: Option[DataFrame] = None
    PluginRegistry.registerSink(new SinkConnector {
      def connectorType = "Capture"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = capturedData = Some(data)
    })

    val pipeline = PipelineConfig(
      name = "TestPipeline",
      dataSources = List(DataSourceConfig("src", "InMemory")),
      transformations = List(
        TransformationConfig("filtered", "SQL", sources = List("src"),
          query = Some("SELECT name FROM src WHERE id > 1"))
      ),
      sinks = List(SinkConfig("out", "Capture", source = Some("filtered")))
    )

    PipelineExecutor.execute(pipeline)

    capturedData shouldBe defined
    capturedData.get.count() shouldBe 2
    capturedData.get.columns should contain only "name"
  }

  it should "execute independent transforms in parallel" in {
    val executionOrder = scala.collection.mutable.ListBuffer[String]()

    PluginRegistry.reset()
    PluginRegistry.registerSource(new SourceConnector {
      def connectorType = "InMemory"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq(("data", 1)).toDF("col", "id")
      }
    })

    PluginRegistry.registerTransform(new TransformPlugin {
      def transformType = "TrackOrder"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = {
        Thread.sleep(100)
        executionOrder.synchronized { executionOrder += config.id }
        inputs.values.head
      }
    })

    PluginRegistry.registerSink(new SinkConnector {
      def connectorType = "Noop"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = ()
    })

    val pipeline = PipelineConfig(
      name = "ParallelTest",
      dataSources = List(DataSourceConfig("A", "InMemory"), DataSourceConfig("B", "InMemory")),
      transformations = List(
        TransformationConfig("T1", "TrackOrder", sources = List("A")),
        TransformationConfig("T2", "TrackOrder", sources = List("B")),
        TransformationConfig("T3", "TrackOrder", sources = List("T1", "T2"))
      ),
      sinks = List(SinkConfig("out", "Noop", source = Some("T3")))
    )

    PipelineExecutor.execute(pipeline)

    executionOrder should contain allOf ("T1", "T2", "T3")
    executionOrder.indexOf("T3") should be > executionOrder.indexOf("T1")
    executionOrder.indexOf("T3") should be > executionOrder.indexOf("T2")
  }
}
