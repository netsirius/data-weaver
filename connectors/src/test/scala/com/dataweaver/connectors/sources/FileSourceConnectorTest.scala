package com.dataweaver.connectors.sources

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileSourceConnectorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[1]").appName("FileTest").getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "FileSourceConnector" should "read CSV files" in {
    val connector = new FileSourceConnector()
    val df = connector.read(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.csv",
      "format" -> "csv"
    ))
    df.count() shouldBe 3
    df.columns should contain allOf ("id", "name", "age")
  }

  it should "read JSON files" in {
    val connector = new FileSourceConnector()
    val df = connector.read(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.json",
      "format" -> "json"
    ))
    df.count() shouldBe 3
  }

  it should "infer format from file extension" in {
    val connector = new FileSourceConnector()
    val df = connector.read(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.csv"
    ))
    df.count() shouldBe 3
  }

  it should "report health check for existing file" in {
    val connector = new FileSourceConnector()
    val result = connector.healthCheck(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.csv"
    ))
    result.isRight shouldBe true
  }

  it should "report health check failure for missing file" in {
    val connector = new FileSourceConnector()
    val result = connector.healthCheck(Map("path" -> "/nonexistent/file.csv"))
    result.isLeft shouldBe true
  }
}
