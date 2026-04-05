package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionsLoaderTest extends AnyFlatSpec with Matchers {

  "ConnectionsLoader" should "load connections from YAML" in {
    val connections = ConnectionsLoader.load("core/src/test/resources/test_connections.yaml")
    connections should contain key "pg-test"
    connections("pg-test")("type") shouldBe "PostgreSQL"
    connections("pg-test")("host") shouldBe "localhost"
  }

  it should "return empty map for non-existent file" in {
    ConnectionsLoader.load("nonexistent.yaml") shouldBe empty
  }

  it should "resolve connection config by name" in {
    val connections = ConnectionsLoader.load("core/src/test/resources/test_connections.yaml")
    val pgConfig = ConnectionsLoader.resolveConnection("pg-test", connections)
    pgConfig("host") shouldBe "localhost"
  }

  it should "throw for unknown connection name" in {
    val connections = ConnectionsLoader.load("core/src/test/resources/test_connections.yaml")
    an[IllegalArgumentException] should be thrownBy
      ConnectionsLoader.resolveConnection("nonexistent", connections)
  }
}
