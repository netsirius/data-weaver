package com.dataweaver.cli

import com.dataweaver.cli.commands.{ApplyCommand, DoctorCommand, ValidateCommand}
import com.dataweaver.core.plugin.{PluginRegistry, SourceConnector, SinkConnector, TransformPlugin, TransformConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val testPipeline = "core/src/test/resources/test_project/pipelines/pipeline.yaml"
  val outputPath = "core/src/test/resources/output_test"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("IntegrationTest").getOrCreate()

    // Register test plugins (overwriting any ServiceLoader-loaded defaults)
    PluginRegistry.registerSource(new SourceConnector {
      def connectorType = "Test"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
        val basePath = sys.props.getOrElse("dataweaver.test.resourcesDir", "core/src/test/resources")
        val id = config.getOrElse("id", "testSource")
        spark.read.option("multiLine", true).json(s"$basePath/input_files/$id.json")
      }
    })

    PluginRegistry.registerTransform(new TransformPlugin {
      def transformType = "SQL"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = {
        val query = config.query.getOrElse(throw new IllegalArgumentException("query required"))
        config.sources.foreach { srcId => inputs(srcId).createOrReplaceTempView(srcId) }
        spark.sql(query)
      }
    })

    PluginRegistry.registerSink(new SinkConnector {
      def connectorType = "Test"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = {
        data.coalesce(1).write.mode("overwrite").json(s"$outputPath/$pipelineName.json")
      }
    })
  }

  override def afterAll(): Unit = {
    deleteDir(outputPath)
    spark.stop()
  }

  private def deleteDir(path: String): Unit = {
    val dir = new java.io.File(path)
    if (dir.exists()) {
      dir.listFiles().foreach { f =>
        if (f.isDirectory) deleteDir(f.getPath) else f.delete()
      }
      dir.delete()
    }
  }

  "DoctorCommand" should "pass all checks for the test pipeline" in {
    val result = DoctorCommand.run(testPipeline)
    result.yamlValid shouldBe true
    result.schemaErrors shouldBe empty
    result.dagValid shouldBe true
  }

  "ValidateCommand" should "validate the test pipeline" in {
    val errors = ValidateCommand.run(testPipeline)
    errors shouldBe empty
  }

  "ApplyCommand" should "execute the test pipeline end-to-end" in {
    ApplyCommand.run(testPipeline)

    val output = spark.read.json(s"$outputPath/ExamplePipeline.json")
    output.count() shouldBe 1
    output.columns should contain only "id"
    output.collect().map(_.getAs[String]("id")) should contain only "Alice"
  }
}
