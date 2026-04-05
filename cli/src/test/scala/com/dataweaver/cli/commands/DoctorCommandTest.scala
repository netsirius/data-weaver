package com.dataweaver.cli.commands

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DoctorCommandTest extends AnyFlatSpec with Matchers {

  "DoctorCommand" should "pass all checks for a valid pipeline" in {
    val result = DoctorCommand.run(
      pipelinePath = "core/src/test/resources/test_project/pipelines/pipeline.yaml",
      connectionsPath = None,
      checkConnections = false
    )
    result.yamlValid shouldBe true
    result.schemaErrors shouldBe empty
    result.dagValid shouldBe true
    result.envErrors shouldBe empty
    result.overallHealthy shouldBe true
  }

  it should "report YAML errors for invalid file" in {
    val result = DoctorCommand.run(
      pipelinePath = "nonexistent.yaml",
      connectionsPath = None,
      checkConnections = false
    )
    result.yamlValid shouldBe false
    result.overallHealthy shouldBe false
  }

  it should "detect missing environment variables" in {
    val result = DoctorCommand.run(
      pipelinePath = "core/src/test/resources/test_project/pipelines/pipeline.yaml",
      connectionsPath = None,
      checkConnections = false,
      envProvider = Map.empty
    )
    result.overallHealthy shouldBe true
  }

  it should "check Java version" in {
    val result = DoctorCommand.run(
      pipelinePath = "core/src/test/resources/test_project/pipelines/pipeline.yaml",
      connectionsPath = None,
      checkConnections = false
    )
    result.javaVersion should not be empty
  }
}
