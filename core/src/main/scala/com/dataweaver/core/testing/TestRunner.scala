package com.dataweaver.core.testing

import com.dataweaver.core.config.{InlineTestConfig, PipelineConfig}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Runs inline tests defined in pipeline YAML and external .test.yaml files.
  *
  * Inline tests evaluate assertions against pipeline results.
  * External tests provide mock data and expected outputs.
  */
object TestRunner {
  private val logger = LogManager.getLogger(getClass)

  /** Result of a single test execution. */
  case class TestResult(
      name: String,
      passed: Boolean,
      message: String = ""
  )

  /** Result of a full test run. */
  case class TestSuiteResult(
      pipelineName: String,
      results: List[TestResult],
      totalTests: Int,
      passed: Int,
      failed: Int
  ) {
    def allPassed: Boolean = failed == 0
  }

  /** Run all inline tests from a pipeline config against execution results.
    * @param config  Pipeline configuration with tests section
    * @param results Map of transform/sink id -> DataFrame (from pipeline execution)
    * @param spark   Implicit SparkSession
    * @return TestSuiteResult with individual test results
    */
  def runInlineTests(
      config: PipelineConfig,
      results: Map[String, DataFrame]
  )(implicit spark: SparkSession): TestSuiteResult = {
    if (config.tests.isEmpty) {
      return TestSuiteResult(config.name, List.empty, 0, 0, 0)
    }

    logger.info(s"Running ${config.tests.size} inline tests for pipeline '${config.name}'")

    val testResults = config.tests.map { test =>
      evaluateInlineTest(test, results)
    }

    val passed = testResults.count(_.passed)
    val failed = testResults.count(!_.passed)

    printTestReport(config.name, testResults)

    TestSuiteResult(config.name, testResults, testResults.size, passed, failed)
  }

  /** Evaluate a single inline test assertion.
    * Assertion format: "target.metric op value"
    * Examples:
    *   sinks.warehouse.row_count > 0
    *   transforms.enriched.missing_count(email) = 0
    *   sinks.out.duplicate_count(id) = 0
    */
  private def evaluateInlineTest(
      test: InlineTestConfig,
      results: Map[String, DataFrame]
  )(implicit spark: SparkSession): TestResult = {
    try {
      val assertion = test.assert.trim

      // Parse: target.metric op value
      val rowCountPattern = """(\w+)\.row_count\s*(>|>=|=|<|<=)\s*(\d+)""".r
      val missingCountPattern = """(\w+)\.missing_count\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r
      val duplicateCountPattern = """(\w+)\.duplicate_count\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r

      // Also support sinks.X.metric and transforms.X.metric prefixes
      val sinkRowCountPattern = """sinks\.(\w+)\.row_count\s*(>|>=|=|<|<=)\s*(\d+)""".r
      val sinkMissingPattern = """sinks\.(\w+)\.missing_count\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r
      val sinkDuplicatePattern = """sinks\.(\w+)\.duplicate_count\((\w+)\)\s*(>|>=|=|<|<=)\s*(\d+)""".r

      assertion match {
        case sinkRowCountPattern(target, op, expected) =>
          evaluateRowCount(test.name, target, op, expected.toLong, results)
        case sinkMissingPattern(target, column, op, expected) =>
          evaluateMissingCount(test.name, target, column, op, expected.toLong, results)
        case sinkDuplicatePattern(target, column, op, expected) =>
          evaluateDuplicateCount(test.name, target, column, op, expected.toLong, results)
        case rowCountPattern(target, op, expected) =>
          evaluateRowCount(test.name, target, op, expected.toLong, results)
        case missingCountPattern(target, column, op, expected) =>
          evaluateMissingCount(test.name, target, column, op, expected.toLong, results)
        case duplicateCountPattern(target, column, op, expected) =>
          evaluateDuplicateCount(test.name, target, column, op, expected.toLong, results)
        case _ =>
          TestResult(test.name, passed = false, s"Cannot parse assertion: '$assertion'")
      }
    } catch {
      case e: Exception =>
        TestResult(test.name, passed = false, s"Error: ${e.getMessage}")
    }
  }

  private def evaluateRowCount(
      name: String, target: String, op: String, expected: Long,
      results: Map[String, DataFrame]
  ): TestResult = {
    val df = findDataFrame(target, results)
    val actual = df.count()
    val passed = compare(actual, op, expected)
    TestResult(name, passed, s"row_count: actual=$actual, expected $op $expected")
  }

  private def evaluateMissingCount(
      name: String, target: String, column: String, op: String, expected: Long,
      results: Map[String, DataFrame]
  )(implicit spark: SparkSession): TestResult = {
    val df = findDataFrame(target, results)
    val actual = df.filter(col(column).isNull || col(column) === "").count()
    val passed = compare(actual, op, expected)
    TestResult(name, passed, s"missing_count($column): actual=$actual, expected $op $expected")
  }

  private def evaluateDuplicateCount(
      name: String, target: String, column: String, op: String, expected: Long,
      results: Map[String, DataFrame]
  )(implicit spark: SparkSession): TestResult = {
    val df = findDataFrame(target, results)
    val total = df.count()
    val distinct = df.select(column).distinct().count()
    val actual = total - distinct
    val passed = compare(actual, op, expected)
    TestResult(name, passed, s"duplicate_count($column): actual=$actual, expected $op $expected")
  }

  private def findDataFrame(target: String, results: Map[String, DataFrame]): DataFrame = {
    results.getOrElse(target,
      throw new IllegalArgumentException(
        s"Test target '$target' not found. Available: ${results.keys.mkString(", ")}"))
  }

  private def compare(actual: Long, op: String, expected: Long): Boolean = op match {
    case ">"  => actual > expected
    case ">=" => actual >= expected
    case "="  => actual == expected
    case "<"  => actual < expected
    case "<=" => actual <= expected
    case _    => false
  }

  private def printTestReport(pipelineName: String, results: List[TestResult]): Unit = {
    println()
    println(s"  Test Results: $pipelineName")
    println(s"  " + "\u2500" * 50)
    results.foreach { r =>
      val status = if (r.passed) "✓ PASS" else "✗ FAIL"
      println(s"    $status  ${r.name}")
      if (!r.passed && r.message.nonEmpty) println(s"           ${r.message}")
    }
    val passed = results.count(_.passed)
    val failed = results.count(!_.passed)
    println()
    println(s"  ${passed} passed, ${failed} failed, ${results.size} total")
    println()
  }
}
