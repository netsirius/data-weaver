package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProfileApplierTest extends AnyFlatSpec with Matchers {

  "ProfileApplier" should "override engine from profile" in {
    val config = PipelineConfig(
      name = "test", engine = "auto",
      profiles = Some(Map(
        "dev" -> Map("engine" -> "local"),
        "prod" -> Map("engine" -> "spark")
      ))
    )
    ProfileApplier.apply(config, "dev").engine shouldBe "local"
    ProfileApplier.apply(config, "prod").engine shouldBe "spark"
  }

  it should "return unchanged config when no profile matches" in {
    val config = PipelineConfig(name = "test", engine = "auto")
    ProfileApplier.apply(config, "nonexistent").engine shouldBe "auto"
  }

  it should "return unchanged config when profiles is None" in {
    val config = PipelineConfig(name = "test", engine = "auto", profiles = None)
    ProfileApplier.apply(config, "dev").engine shouldBe "auto"
  }
}
