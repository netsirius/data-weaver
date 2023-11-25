package com.dataweaver.reader

import com.dataweaver.config.DataSourceConfig
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

class TestReader(config: DataSourceConfig) extends DataReader {

  override def sourceName: String = config.id

  override def readData()(implicit spark: SparkSession): DataFrame = {
    val filePath = s"src/test/resources/input_files/${config.id}.json"

    val json = {
      val source = Source.fromFile(filePath)
      try source.mkString finally source.close()
    }

    val schema = inferSchemaFromJson(json)

    spark.read
      .option("multiLine", true)
      .schema(schema)
      .json(filePath)
  }

  private def inferSchemaFromJson(jsonString: String): StructType = {
    implicit val formats: Formats = DefaultFormats

    // Analizar el primer objeto del JSON
    val firstObject = (parse(jsonString).children.headOption.getOrElse(JNothing)).extract[Map[String, Any]]

    // Inferir tipos de datos basándose en el primer objeto
    val fields = firstObject.keys.map { key =>
      val dataType = inferDataType(firstObject(key))
      StructField(key, dataType, nullable = true)
    }

    StructType(fields.toArray)
  }

  private def inferDataType(value: Any): DataType = value match {
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Double => DoubleType
    case _: Boolean => BooleanType
    case _: String => StringType
    case _ => StringType // Tipo por defecto
  }
}
