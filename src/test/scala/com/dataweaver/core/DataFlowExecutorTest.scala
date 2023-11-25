package com.dataweaver.core

import com.dataweaver.config.{DataWeaverConfig, ExecutionMode}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataFlowExecutorTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {

  val bqOutputPath = "./src/test/resources/output_test"

  private def deleteBqOutputDir(output: String): Unit = {
    import java.io.File
    import scala.reflect.io.Directory
    val directory = new Directory(new File(output))
    directory.deleteRecursively()
  }

  override def afterAll(): Unit = deleteBqOutputDir(bqOutputPath)

  /**
   * Test for the DataFlowExecutor class.
   */
  "DataFlowExecutor" should "correctly execute a data flow" in {
    implicit val spark: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val appConfig = DataWeaverConfig.load("src/test/resources/test_project/config").get
    val executor = new DataFlowExecutor(appConfig)
    executor.executeDataFlows(None, None, ExecutionMode.Test)

    val output_df = spark.read.json("src/test/resources/output_test/ExamplePipeline.json")

    // Assertions
    output_df.count() should be(1)
    output_df.columns should contain only ("id")
    output_df.collect().map(_.getAs[String]("id")) should contain only ("Alice")

    spark.stop()
  }

}
