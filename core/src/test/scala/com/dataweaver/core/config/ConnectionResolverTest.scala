package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionResolverTest extends AnyFlatSpec with Matchers {

  "ConnectionResolver" should "resolve env vars" in {
    val resolver = new ConnectionResolver(
      envProvider = Map("DB_HOST" -> "localhost", "DB_PORT" -> "5432")
    )
    resolver.resolve("${env.DB_HOST}") shouldBe "localhost"
    resolver.resolve("${env.DB_PORT}") shouldBe "5432"
  }

  it should "leave non-variable strings unchanged" in {
    val resolver = new ConnectionResolver(envProvider = Map.empty)
    resolver.resolve("plain-value") shouldBe "plain-value"
  }

  it should "resolve multiple variables in one string" in {
    val resolver = new ConnectionResolver(
      envProvider = Map("HOST" -> "db.example.com", "PORT" -> "5432")
    )
    resolver.resolve("jdbc:postgresql://${env.HOST}:${env.PORT}/mydb") shouldBe
      "jdbc:postgresql://db.example.com:5432/mydb"
  }

  it should "throw on unresolvable variable" in {
    val resolver = new ConnectionResolver(envProvider = Map.empty)
    an[IllegalArgumentException] should be thrownBy resolver.resolve("${env.MISSING}")
  }

  it should "resolve a full config map" in {
    val resolver = new ConnectionResolver(
      envProvider = Map("HOST" -> "prod-db", "PASS" -> "secret")
    )
    val config = Map("host" -> "${env.HOST}", "password" -> "${env.PASS}", "port" -> "5432")
    val resolved = resolver.resolveMap(config)
    resolved shouldBe Map("host" -> "prod-db", "password" -> "secret", "port" -> "5432")
  }
}
