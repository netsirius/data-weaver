package com.dataweaver.runner

import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LocalSparkRunnerTest extends AnyFlatSpec with Matchers with MockitoSugar {
  "LocalSparkRunner" should "execute in test mode" in {
    // Cargar el archivo YAML
    val confPath = "src/test/resources/test_project/config"
    val runner = new LocalSparkRunner("test", "TestApp")
    runner.run(confPath, None, None)
  }
}
