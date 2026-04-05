package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDate

class VariableResolverTest extends AnyFlatSpec with Matchers {

  val resolver = new VariableResolver(
    envProvider = Map("DB_HOST" -> "localhost"),
    dateProvider = () => LocalDate.of(2026, 4, 5)
  )

  "VariableResolver" should "resolve env vars" in {
    resolver.resolve("${env.DB_HOST}") shouldBe "localhost"
  }

  it should "resolve date.today" in {
    resolver.resolve("${date.today}") shouldBe "2026-04-05"
  }

  it should "resolve date.yesterday" in {
    resolver.resolve("${date.yesterday}") shouldBe "2026-04-04"
  }

  it should "resolve date.format" in {
    resolver.resolve("${date.format('yyyy/MM/dd')}") shouldBe "2026/04/05"
  }

  it should "resolve date.offset" in {
    resolver.resolve("${date.offset(-7)}") shouldBe "2026-03-29"
  }

  it should "resolve multiple variables" in {
    resolver.resolve("host=${env.DB_HOST}&date=${date.today}") shouldBe "host=localhost&date=2026-04-05"
  }

  it should "leave plain strings unchanged" in {
    resolver.resolve("no-vars") shouldBe "no-vars"
  }

  it should "throw on undefined env var" in {
    an[IllegalArgumentException] should be thrownBy resolver.resolve("${env.MISSING}")
  }

  it should "resolve a config map" in {
    val config = Map("host" -> "${env.DB_HOST}", "since" -> "${date.yesterday}")
    resolver.resolveMap(config) shouldBe Map("host" -> "localhost", "since" -> "2026-04-04")
  }
}
