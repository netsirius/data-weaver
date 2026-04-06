package com.dataweaver.transformations.quality

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Data quality gate transformation.
  * Runs checks against the input DataFrame and passes it through unchanged.
  * Checks are defined as strings in the pipeline YAML:
  *   - row_count > 0
  *   - missing_count(column) = 0
  *   - duplicate_count(column) = 0
  *   - string_length(column) > N
  *
  * onFail behavior:
  *   - "abort": throws exception, stops pipeline
  *   - "warn":  logs warning, continues
  *   - "skip":  silently continues
  */
class DataQualityPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)

  def transformType: String = "DataQuality"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val df = inputs.values.headOption.getOrElse(
      throw new IllegalArgumentException(s"DataQuality '${config.id}' requires at least one input"))

    val checks = config.extra.get("checks") match {
      case Some(checksStr) => checksStr.split("\\|").map(_.trim).toList
      case None => List.empty
    }

    val onFail = config.extra.getOrElse("onFail", "abort")

    if (checks.isEmpty) {
      logger.warn(s"DataQuality '${config.id}' has no checks defined")
      return df
    }

    val results = checks.map(check => (check, evaluateCheck(df, check)))

    // Print report
    printReport(config.id, results)

    // Handle failures
    val failures = results.filter(!_._2.passed)
    if (failures.nonEmpty) {
      val failureMsg = failures.map { case (check, r) =>
        s"  ✗ $check (actual: ${r.actual})"
      }.mkString("\n")

      onFail match {
        case "abort" =>
          throw new DataQualityException(
            s"Data quality checks failed for '${config.id}':\n$failureMsg")
        case "warn" =>
          logger.warn(s"Data quality warnings for '${config.id}':\n$failureMsg")
        case "skip" =>
          // silently continue
        case other =>
          logger.warn(s"Unknown onFail value '$other', treating as 'warn'")
          logger.warn(s"Data quality warnings for '${config.id}':\n$failureMsg")
      }
    }

    // Passthrough — the DataFrame is unchanged
    df
  }

  /** Evaluate a single check expression against a DataFrame. */
  private def evaluateCheck(df: DataFrame, check: String): CheckResult = {
    val rowCountPattern = """row_count\s*(>|>=|=|<|<=)\s*(\d+)""".r
    val missingCountPattern = """missing_count\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r
    val duplicateCountPattern = """duplicate_count\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r
    val stringLengthPattern = """string_length\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r

    check match {
      case rowCountPattern(op, expected) =>
        val actual = df.count()
        CheckResult(compare(actual, op, expected.toLong), actual.toString)

      case missingCountPattern(column, op, expected) =>
        val actual = df.filter(col(column).isNull || col(column) === "").count()
        CheckResult(compare(actual, op, expected.toLong), actual.toString)

      case duplicateCountPattern(column, op, expected) =>
        val total = df.count()
        val distinct = df.select(column).distinct().count()
        val actual = total - distinct
        CheckResult(compare(actual, op, expected.toLong), actual.toString)

      case stringLengthPattern(column, op, expected) =>
        val minLen = df.select(min(length(col(column)))).first().getLong(0)
        CheckResult(compare(minLen, op, expected.toLong), minLen.toString)

      case _ =>
        logger.warn(s"Unknown check expression: '$check'")
        CheckResult(passed = false, "unknown check")
    }
  }

  private def compare(actual: Long, op: String, expected: Long): Boolean = op match {
    case ">"  => actual > expected
    case ">=" => actual >= expected
    case "="  => actual == expected
    case "<"  => actual < expected
    case "<=" => actual <= expected
    case _    => false
  }

  private def printReport(id: String, results: List[(String, CheckResult)]): Unit = {
    println()
    println(s"  Quality checks for '$id':")
    results.foreach { case (check, result) =>
      val status = if (result.passed) "✓" else "✗"
      println(s"    $status $check (actual: ${result.actual})")
    }
    println()
  }
}

case class CheckResult(passed: Boolean, actual: String)

class DataQualityException(message: String) extends RuntimeException(message)
