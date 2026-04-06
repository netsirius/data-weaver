package com.dataweaver.cli.commands

import com.dataweaver.core.config.{PipelineConfig, ProfileApplier, YAMLParser}
import com.dataweaver.core.engine.{EngineSelector, PipelineExecutor}
import com.dataweaver.core.testing.TestRunner
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestCommand {

  /** Run tests for a pipeline.
    * Executes the pipeline in test mode, then runs inline tests against results.
    * @param pipelinePath Path to pipeline YAML
    * @param autoGenerate If true, generate tests from schema inference (future)
    * @param coverage     If true, show test coverage report
    */
  def run(
      pipelinePath: String,
      autoGenerate: Boolean = false,
      coverage: Boolean = false
  ): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    if (coverage) {
      showCoverage(config)
      return
    }

    if (autoGenerate) {
      println("Auto-generation of tests is not yet implemented.")
      println("Define tests manually in the pipeline YAML:")
      println()
      println("tests:")
      println("  - name: \"has data\"")
      println("    assert: sinks.<sink_id>.row_count > 0")
      return
    }

    if (config.tests.isEmpty) {
      println(s"No tests defined in pipeline '${config.name}'.")
      println("Add a 'tests:' section to your pipeline YAML.")
      return
    }

    // Execute pipeline to get results, then run tests
    val engine = EngineSelector.select(config)
    implicit val spark: SparkSession = engine.getOrCreateSession("DataWeaver-Test")

    try {
      println(s"Executing pipeline '${config.name}' for testing...")

      // Execute and capture results
      val results = PipelineExecutor.executeAndCapture(config)

      // Run inline tests
      val testResult = TestRunner.runInlineTests(config, results)

      if (!testResult.allPassed) {
        System.err.println(s"${testResult.failed} test(s) FAILED")
        sys.exit(1)
      }
    } finally {
      engine.stop()
    }
  }

  /** Show which transforms/sinks have tests defined. */
  private def showCoverage(config: PipelineConfig): Unit = {
    val allIds = config.dataSources.map(_.id) ++
      config.transformations.map(_.id) ++
      config.sinks.map(_.id)

    val testedIds = config.tests.flatMap { test =>
      // Extract target from assertion
      val pattern = """(?:sinks\.|transforms\.)?(\w+)\.""".r
      pattern.findFirstMatchIn(test.assert).map(_.group(1))
    }.toSet

    println()
    println(s"  Test Coverage: ${config.name}")
    println(s"  " + "\u2500" * 40)
    allIds.foreach { id =>
      val status = if (testedIds.contains(id)) "✓ tested" else "○ no tests"
      println(s"    $status  $id")
    }
    val covered = allIds.count(testedIds.contains)
    val total = allIds.size
    val pct = if (total > 0) (covered * 100) / total else 0
    println()
    println(s"  Coverage: $covered/$total ($pct%)")
    println()
  }
}
