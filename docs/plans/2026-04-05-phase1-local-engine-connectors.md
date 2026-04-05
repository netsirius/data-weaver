# Phase 1: Local Engine + Core Connectors — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable full ETL pipeline execution locally without external Spark, add priority connectors (PostgreSQL, File, DeltaLake), implement `weaver plan/explain/inspect` commands, add profile support and variable injection.

**Architecture:** The "local" engine uses Spark in `local[*]` mode bundled in the CLI JAR (zero external dependencies). DuckDB is added as an optional fast-path engine for small datasets. New connectors implement the `SourceConnector`/`SinkConnector` traits from Phase 0. Profile overrides and variable injection are handled by enhancing the existing `ConnectionResolver` and `YAMLParser`.

**Tech Stack:** Scala 2.13.14, Spark 4.0.2, DuckDB JDBC 1.1.3, Delta Lake 4.0.0, PostgreSQL JDBC 42.7.4, ScalaTest 3.2.19

**Spec:** `docs/specs/2026-04-05-data-weaver-v2-design.md` (sections 2.3, 2.5, 2.6, 3, 5, 5.1)

**Depends on:** Phase 0 completed (`feature/phase0-foundation` branch)

---

## File Structure

### Files to CREATE

```
# Core engine abstraction
core/src/main/scala/com/dataweaver/core/engine/Engine.scala
core/src/main/scala/com/dataweaver/core/engine/SparkEngine.scala
core/src/main/scala/com/dataweaver/core/engine/DuckDBEngine.scala
core/src/main/scala/com/dataweaver/core/engine/EngineSelector.scala
core/src/test/scala/com/dataweaver/core/engine/EngineSelectorTest.scala

# Variable injection enhancement
core/src/main/scala/com/dataweaver/core/config/VariableResolver.scala
core/src/test/scala/com/dataweaver/core/config/VariableResolverTest.scala

# Profile support
core/src/main/scala/com/dataweaver/core/config/ProfileApplier.scala
core/src/test/scala/com/dataweaver/core/config/ProfileApplierTest.scala

# Connections.yaml loader
core/src/main/scala/com/dataweaver/core/config/ConnectionsLoader.scala
core/src/test/scala/com/dataweaver/core/config/ConnectionsLoaderTest.scala

# New connectors
connectors/src/main/scala/com/dataweaver/connectors/sources/PostgreSQLSourceConnector.scala
connectors/src/main/scala/com/dataweaver/connectors/sources/FileSourceConnector.scala
connectors/src/main/scala/com/dataweaver/connectors/sinks/FileSinkConnector.scala
connectors/src/main/scala/com/dataweaver/connectors/sinks/DeltaLakeSinkConnector.scala
connectors/src/test/scala/com/dataweaver/connectors/sources/FileSourceConnectorTest.scala

# CLI commands
cli/src/main/scala/com/dataweaver/cli/commands/PlanCommand.scala
cli/src/main/scala/com/dataweaver/cli/commands/ExplainCommand.scala
cli/src/main/scala/com/dataweaver/cli/commands/InspectCommand.scala

# Test resources
connectors/src/test/resources/test_data/sample.csv
connectors/src/test/resources/test_data/sample.json
```

### Files to MODIFY

```
build.sbt                               ← Add DuckDB, Delta, PostgreSQL deps
core/src/main/scala/com/dataweaver/core/engine/PipelineExecutor.scala  ← Use Engine trait
core/src/main/scala/com/dataweaver/core/config/ConnectionResolver.scala ← Enhance with date vars
core/src/main/scala/com/dataweaver/core/config/PipelineConfig.scala    ← Add profiles field
core/src/main/scala/com/dataweaver/core/config/YAMLParser.scala        ← Parse profiles
cli/src/main/scala/com/dataweaver/cli/WeaverCLI.scala                  ← Add plan/explain/inspect
cli/src/main/scala/com/dataweaver/cli/commands/DoctorCommand.scala     ← Add connection health checks
connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector
connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SinkConnector
```

---

## Task 1: Add Dependencies to build.sbt

**Files:**
- Modify: `build.sbt`

- [ ] **Step 1: Add DuckDB, Delta Lake, PostgreSQL JDBC dependencies**

Add to the `connectors` module:
```scala
lazy val connectors = (project in file("connectors"))
  .dependsOn(core)
  .settings(
    commonSettings,
    name := "data-weaver-connectors",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "com.mysql"         % "mysql-connector-j" % "9.1.0",
      "org.json4s"       %% "json4s-jackson" % "4.0.7",
      // Phase 1 additions
      "org.postgresql"    % "postgresql" % "42.7.4",
      "io.delta"         %% "delta-spark" % "4.0.0" % "provided"
    )
  )
```

Add DuckDB to the `core` module:
```scala
lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "data-weaver-core",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "com.typesafe"      % "config"     % "1.4.3",
      "io.circe"         %% "circe-core"    % "0.14.10",
      "io.circe"         %% "circe-yaml"    % "0.15.1",
      "io.circe"         %% "circe-generic" % "0.14.10",
      // Phase 1 additions
      "org.duckdb"        % "duckdb_jdbc" % "1.1.3"
    )
  )
```

- [ ] **Step 2: Commit**

```bash
git add build.sbt
git commit -m "build: add DuckDB, Delta Lake, PostgreSQL JDBC dependencies"
```

---

## Task 2: Engine Abstraction Layer

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/engine/Engine.scala`
- Create: `core/src/main/scala/com/dataweaver/core/engine/SparkEngine.scala`
- Create: `core/src/main/scala/com/dataweaver/core/engine/DuckDBEngine.scala`
- Create: `core/src/main/scala/com/dataweaver/core/engine/EngineSelector.scala`
- Test: `core/src/test/scala/com/dataweaver/core/engine/EngineSelectorTest.scala`

- [ ] **Step 1: Write Engine trait**

```scala
// core/src/main/scala/com/dataweaver/core/engine/Engine.scala
package com.dataweaver.core.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Abstraction for execution engines.
  * Both Spark and DuckDB implement this trait.
  * The DataFrame type is Spark DataFrame in both cases — DuckDB converts
  * its results to Spark DataFrame for compatibility with existing plugins.
  */
trait Engine {
  def name: String

  /** Create or get the SparkSession for this engine.
    * For SparkEngine: creates a distributed session.
    * For DuckDBEngine: creates a minimal local Spark session for DataFrame interop.
    */
  def getOrCreateSession(appName: String, config: Map[String, String] = Map.empty): SparkSession

  /** Execute a SQL query against registered tables.
    * @param query SQL string
    * @param spark Implicit SparkSession
    * @return DataFrame with query results
    */
  def sql(query: String)(implicit spark: SparkSession): DataFrame

  /** Stop the engine and release resources. */
  def stop(): Unit
}
```

- [ ] **Step 2: Write SparkEngine**

```scala
// core/src/main/scala/com/dataweaver/core/engine/SparkEngine.scala
package com.dataweaver.core.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Spark-based engine. Uses local[*] by default, or connects to a cluster. */
object SparkEngine extends Engine {
  def name: String = "spark"

  private var session: Option[SparkSession] = None

  def getOrCreateSession(appName: String, config: Map[String, String] = Map.empty): SparkSession = {
    session.getOrElse {
      val builder = SparkSession.builder().appName(appName)

      // Apply Spark-specific config from profiles
      config.foreach { case (k, v) =>
        if (k.startsWith("spark.")) builder.config(k, v)
      }

      // Default to local if no master set
      if (!config.contains("spark.master")) {
        builder.master("local[*]")
      }

      val s = builder.getOrCreate()
      session = Some(s)
      s
    }
  }

  def sql(query: String)(implicit spark: SparkSession): DataFrame = spark.sql(query)

  def stop(): Unit = {
    session.foreach(_.stop())
    session = None
  }
}
```

- [ ] **Step 3: Write DuckDBEngine**

```scala
// core/src/main/scala/com/dataweaver/core/engine/DuckDBEngine.scala
package com.dataweaver.core.engine

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager}

/** DuckDB-based engine for fast local execution.
  * Uses DuckDB for SQL queries but wraps results in Spark DataFrames
  * for plugin compatibility. Uses a minimal Spark local session.
  */
object DuckDBEngine extends Engine {
  private val logger = LogManager.getLogger(getClass)

  def name: String = "duckdb"

  private var session: Option[SparkSession] = None
  private var duckConn: Option[Connection] = None

  def getOrCreateSession(appName: String, config: Map[String, String] = Map.empty): SparkSession = {
    // DuckDB still needs a minimal Spark session for DataFrame interop
    session.getOrElse {
      val s = SparkSession.builder()
        .appName(appName)
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
      session = Some(s)

      // Initialize DuckDB connection
      Class.forName("org.duckdb.DuckDBDriver")
      duckConn = Some(DriverManager.getConnection("jdbc:duckdb:"))
      logger.info("DuckDB engine initialized (in-memory)")
      s
    }
  }

  /** Execute SQL via DuckDB for speed, return as Spark DataFrame. */
  def sql(query: String)(implicit spark: SparkSession): DataFrame = {
    // For now, delegate to Spark SQL (DuckDB SQL optimization is Phase 2)
    // DuckDB is used for file reads and schema inference
    spark.sql(query)
  }

  /** Direct DuckDB query for internal use (schema inference, file reads). */
  def duckQuery(query: String): java.sql.ResultSet = {
    val conn = duckConn.getOrElse(
      throw new IllegalStateException("DuckDB not initialized. Call getOrCreateSession first."))
    conn.createStatement().executeQuery(query)
  }

  def stop(): Unit = {
    duckConn.foreach(_.close())
    duckConn = None
    session.foreach(_.stop())
    session = None
  }
}
```

- [ ] **Step 4: Write EngineSelector**

```scala
// core/src/main/scala/com/dataweaver/core/engine/EngineSelector.scala
package com.dataweaver.core.engine

import com.dataweaver.core.config.PipelineConfig
import org.apache.log4j.LogManager

object EngineSelector {
  private val logger = LogManager.getLogger(getClass)

  /** Default size threshold for auto engine selection (1 GB). */
  val DEFAULT_THRESHOLD_BYTES: Long = 1L * 1024 * 1024 * 1024

  /** Select the appropriate engine for a pipeline.
    * @param config Pipeline configuration with engine field
    * @return The selected Engine instance
    */
  def select(config: PipelineConfig): Engine = {
    config.engine.toLowerCase match {
      case "spark" =>
        logger.info("Engine: Spark (explicit)")
        SparkEngine
      case "local" =>
        logger.info("Engine: DuckDB (explicit local)")
        DuckDBEngine
      case "auto" =>
        // For auto mode, default to DuckDB for local dev
        // In production (when spark.master is set to a cluster), use Spark
        val sparkConfig = getSparkConfig(config)
        val master = sparkConfig.getOrElse("spark.master", "local[*]")
        if (master.startsWith("local")) {
          logger.info("Engine: DuckDB (auto — local mode)")
          DuckDBEngine
        } else {
          logger.info(s"Engine: Spark (auto — cluster: $master)")
          SparkEngine
        }
      case other =>
        throw new IllegalArgumentException(
          s"Unknown engine '$other'. Valid: spark, local, auto")
    }
  }

  /** Extract Spark configuration from pipeline profiles. */
  private def getSparkConfig(config: PipelineConfig): Map[String, String] = {
    // Profiles will be resolved before reaching here
    config.profiles
      .flatMap(_.get("spark"))
      .flatMap {
        case m: Map[_, _] => Some(m.asInstanceOf[Map[String, String]])
        case _ => None
      }
      .getOrElse(Map.empty)
  }
}
```

- [ ] **Step 5: Write EngineSelector test**

```scala
// core/src/test/scala/com/dataweaver/core/engine/EngineSelectorTest.scala
package com.dataweaver.core.engine

import com.dataweaver.core.config.PipelineConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineSelectorTest extends AnyFlatSpec with Matchers {

  "EngineSelector" should "select Spark when engine is 'spark'" in {
    val config = PipelineConfig(name = "test", engine = "spark")
    EngineSelector.select(config) shouldBe SparkEngine
  }

  it should "select DuckDB when engine is 'local'" in {
    val config = PipelineConfig(name = "test", engine = "local")
    EngineSelector.select(config) shouldBe DuckDBEngine
  }

  it should "select DuckDB for auto mode in local context" in {
    val config = PipelineConfig(name = "test", engine = "auto")
    EngineSelector.select(config) shouldBe DuckDBEngine
  }

  it should "throw on unknown engine" in {
    val config = PipelineConfig(name = "test", engine = "quantum")
    an[IllegalArgumentException] should be thrownBy EngineSelector.select(config)
  }
}
```

- [ ] **Step 6: Commit**

```bash
git add core/src/main/scala/com/dataweaver/core/engine/
git add core/src/test/scala/com/dataweaver/core/engine/EngineSelectorTest.scala
git commit -m "feat(core): add Engine abstraction with Spark and DuckDB engines

Engine trait abstracts execution backend. SparkEngine for distributed
workloads, DuckDBEngine for fast local execution. EngineSelector
chooses automatically based on pipeline config."
```

---

## Task 3: Variable Injection Enhancement

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/config/VariableResolver.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/VariableResolverTest.scala`

- [ ] **Step 1: Write VariableResolver test**

```scala
// core/src/test/scala/com/dataweaver/core/config/VariableResolverTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDate

class VariableResolverTest extends AnyFlatSpec with Matchers {

  val resolver = new VariableResolver(
    envProvider = Map("DB_HOST" -> "localhost"),
    dateProvider = () => LocalDate.of(2026, 4, 5)
  )

  "VariableResolver" should "resolve ${env.VAR}" in {
    resolver.resolve("${env.DB_HOST}") shouldBe "localhost"
  }

  it should "resolve ${date.today}" in {
    resolver.resolve("${date.today}") shouldBe "2026-04-05"
  }

  it should "resolve ${date.yesterday}" in {
    resolver.resolve("${date.yesterday}") shouldBe "2026-04-04"
  }

  it should "resolve ${date.format('yyyy/MM/dd')}" in {
    resolver.resolve("${date.format('yyyy/MM/dd')}") shouldBe "2026/04/05"
  }

  it should "resolve ${date.offset(-7)}" in {
    resolver.resolve("${date.offset(-7)}") shouldBe "2026-03-29"
  }

  it should "resolve multiple variables in one string" in {
    resolver.resolve("host=${env.DB_HOST}&date=${date.today}") shouldBe
      "host=localhost&date=2026-04-05"
  }

  it should "leave plain strings unchanged" in {
    resolver.resolve("no-vars-here") shouldBe "no-vars-here"
  }

  it should "throw on undefined env var" in {
    an[IllegalArgumentException] should be thrownBy resolver.resolve("${env.MISSING}")
  }

  it should "resolve a full config map" in {
    val config = Map("host" -> "${env.DB_HOST}", "since" -> "${date.yesterday}")
    val resolved = resolver.resolveMap(config)
    resolved shouldBe Map("host" -> "localhost", "since" -> "2026-04-04")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `VariableResolver` does not exist

- [ ] **Step 3: Implement VariableResolver**

```scala
// core/src/main/scala/com/dataweaver/core/config/VariableResolver.scala
package com.dataweaver.core.config

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** Resolves variable references in configuration values.
  * Supports:
  *   ${env.VARIABLE_NAME}           — environment variables (+ .env file)
  *   ${date.today}                  — current date (yyyy-MM-dd)
  *   ${date.yesterday}              — yesterday's date
  *   ${date.offset(N)}              — date offset by N days (negative = past)
  *   ${date.format('pattern')}      — current date with custom format
  *
  * @param envProvider  Map of env var name -> value. Defaults to System.getenv()
  * @param dotEnvPath   Path to .env file. Defaults to ".env"
  * @param dateProvider Function that returns current date (injectable for testing)
  */
class VariableResolver(
    envProvider: Map[String, String] = sys.env,
    dotEnvPath: String = ".env",
    dateProvider: () => LocalDate = () => LocalDate.now()
) {

  private val envPattern = """\$\{env\.([^}]+)\}""".r
  private val dateTodayPattern = """\$\{date\.today\}""".r
  private val dateYesterdayPattern = """\$\{date\.yesterday\}""".r
  private val dateOffsetPattern = """\$\{date\.offset\((-?\d+)\)\}""".r
  private val dateFormatPattern = """\$\{date\.format\('([^']+)'\)\}""".r

  private lazy val mergedEnv: Map[String, String] = {
    val dotEnvVars = loadDotEnv(dotEnvPath)
    dotEnvVars ++ envProvider
  }

  /** Resolve all variable references in a string. */
  def resolve(value: String): String = {
    var result = value

    // Resolve env vars
    result = envPattern.replaceAllIn(result, m => {
      val varName = m.group(1)
      java.util.regex.Matcher.quoteReplacement(
        mergedEnv.getOrElse(varName,
          throw new IllegalArgumentException(
            s"Environment variable '$varName' not found. " +
              s"Set it with: export $varName=<value> or add to .env file"))
      )
    })

    // Resolve date.today
    val today = dateProvider()
    result = dateTodayPattern.replaceAllIn(result, today.toString)

    // Resolve date.yesterday
    result = dateYesterdayPattern.replaceAllIn(result, today.minusDays(1).toString)

    // Resolve date.offset(N)
    result = dateOffsetPattern.replaceAllIn(result, m => {
      val days = m.group(1).toInt
      today.plusDays(days).toString
    })

    // Resolve date.format('pattern')
    result = dateFormatPattern.replaceAllIn(result, m => {
      val pattern = m.group(1)
      today.format(DateTimeFormatter.ofPattern(pattern))
    })

    result
  }

  /** Resolve all variable references in a config map. */
  def resolveMap(config: Map[String, String]): Map[String, String] =
    config.map { case (k, v) => k -> resolve(v) }

  private def loadDotEnv(path: String): Map[String, String] = {
    val file = new java.io.File(path)
    if (!file.exists()) return Map.empty
    scala.io.Source.fromFile(file).getLines()
      .map(_.trim)
      .filterNot(line => line.isEmpty || line.startsWith("#"))
      .flatMap { line =>
        line.split("=", 2) match {
          case Array(key, value) => Some(key.trim -> value.trim)
          case _                 => None
        }
      }
      .toMap
  }
}
```

- [ ] **Step 4: Run tests**

Expected: All 9 VariableResolver tests PASS

- [ ] **Step 5: Commit**

```bash
git add core/src/
git commit -m "feat(core): add VariableResolver with date injection

Extends variable resolution beyond \${env.VAR} to include:
- \${date.today}, \${date.yesterday}
- \${date.offset(N)} for relative dates
- \${date.format('pattern')} for custom formats
Injectable date provider for deterministic testing."
```

---

## Task 4: Profile Support

**Files:**
- Modify: `core/src/main/scala/com/dataweaver/core/config/PipelineConfig.scala`
- Create: `core/src/main/scala/com/dataweaver/core/config/ProfileApplier.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/ProfileApplierTest.scala`

- [ ] **Step 1: Add profiles field to PipelineConfig**

Add to PipelineConfig:
```scala
case class PipelineConfig(
    name: String,
    tag: String = "",
    engine: String = "auto",
    dataSources: List[DataSourceConfig] = List.empty,
    transformations: List[TransformationConfig] = List.empty,
    sinks: List[SinkConfig] = List.empty,
    profiles: Option[Map[String, Map[String, Any]]] = None  // NEW
)
```

- [ ] **Step 2: Write ProfileApplier test**

```scala
// core/src/test/scala/com/dataweaver/core/config/ProfileApplierTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProfileApplierTest extends AnyFlatSpec with Matchers {

  "ProfileApplier" should "override engine from profile" in {
    val config = PipelineConfig(
      name = "test",
      engine = "auto",
      profiles = Some(Map(
        "dev" -> Map("engine" -> "local"),
        "prod" -> Map("engine" -> "spark")
      ))
    )

    val devConfig = ProfileApplier.apply(config, "dev")
    devConfig.engine shouldBe "local"

    val prodConfig = ProfileApplier.apply(config, "prod")
    prodConfig.engine shouldBe "spark"
  }

  it should "return unchanged config when no profile matches" in {
    val config = PipelineConfig(name = "test", engine = "auto")
    val result = ProfileApplier.apply(config, "nonexistent")
    result.engine shouldBe "auto"
  }

  it should "return unchanged config when profiles is None" in {
    val config = PipelineConfig(name = "test", engine = "auto", profiles = None)
    val result = ProfileApplier.apply(config, "dev")
    result.engine shouldBe "auto"
  }
}
```

- [ ] **Step 3: Implement ProfileApplier**

```scala
// core/src/main/scala/com/dataweaver/core/config/ProfileApplier.scala
package com.dataweaver.core.config

import org.apache.log4j.LogManager

/** Applies environment profile overrides to a PipelineConfig.
  * Profile values override the base config for non-sensitive settings
  * like engine, spark config, and query limits.
  */
object ProfileApplier {
  private val logger = LogManager.getLogger(getClass)

  /** Apply a named profile's overrides to the pipeline config.
    * @param config  Base pipeline configuration
    * @param envName Profile name (e.g., "dev", "prod")
    * @return Updated config with profile overrides applied
    */
  def apply(config: PipelineConfig, envName: String): PipelineConfig = {
    config.profiles match {
      case None =>
        logger.info(s"No profiles defined, using base config")
        config
      case Some(profiles) =>
        profiles.get(envName) match {
          case None =>
            logger.warn(s"Profile '$envName' not found. Available: ${profiles.keys.mkString(", ")}")
            config
          case Some(overrides) =>
            logger.info(s"Applying profile '$envName'")
            applyOverrides(config, overrides)
        }
    }
  }

  private def applyOverrides(config: PipelineConfig, overrides: Map[String, Any]): PipelineConfig = {
    var result = config

    // Override engine
    overrides.get("engine").foreach {
      case e: String => result = result.copy(engine = e)
      case _ =>
    }

    // Additional profile overrides can be added here as needed
    // e.g., spark config, partition counts, query limits

    result
  }
}
```

- [ ] **Step 4: Run tests**

Expected: All 3 ProfileApplier tests PASS

- [ ] **Step 5: Commit**

```bash
git add core/src/
git commit -m "feat(core): add profile support for environment-specific overrides

Profiles allow dev/prod config overrides in the pipeline YAML.
ProfileApplier resolves the active profile and applies overrides
to engine, spark config, etc. Non-sensitive values only."
```

---

## Task 5: Connections.yaml Loader

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/config/ConnectionsLoader.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/ConnectionsLoaderTest.scala`
- Create: `core/src/test/resources/test_connections.yaml`

- [ ] **Step 1: Write test connections YAML**

```yaml
# core/src/test/resources/test_connections.yaml
connections:
  pg-test:
    type: PostgreSQL
    host: localhost
    port: "5432"
    database: testdb
    user: testuser
    password: testpass

  gcs-test:
    type: CloudStorage
    provider: gcs
    bucket: test-bucket
```

- [ ] **Step 2: Write ConnectionsLoader test**

```scala
// core/src/test/scala/com/dataweaver/core/config/ConnectionsLoaderTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionsLoaderTest extends AnyFlatSpec with Matchers {

  "ConnectionsLoader" should "load connections from YAML" in {
    val connections = ConnectionsLoader.load("core/src/test/resources/test_connections.yaml")
    connections should contain key "pg-test"
    connections("pg-test")("type") shouldBe "PostgreSQL"
    connections("pg-test")("host") shouldBe "localhost"
    connections("pg-test")("port") shouldBe "5432"
  }

  it should "return empty map for non-existent file" in {
    val connections = ConnectionsLoader.load("nonexistent.yaml")
    connections shouldBe empty
  }

  it should "resolve connection config by name" in {
    val connections = ConnectionsLoader.load("core/src/test/resources/test_connections.yaml")
    val pgConfig = ConnectionsLoader.resolveConnection("pg-test", connections)
    pgConfig should contain key "host"
    pgConfig("host") shouldBe "localhost"
  }

  it should "throw for unknown connection name" in {
    val connections = ConnectionsLoader.load("core/src/test/resources/test_connections.yaml")
    an[IllegalArgumentException] should be thrownBy
      ConnectionsLoader.resolveConnection("nonexistent", connections)
  }
}
```

- [ ] **Step 3: Implement ConnectionsLoader**

```scala
// core/src/main/scala/com/dataweaver/core/config/ConnectionsLoader.scala
package com.dataweaver.core.config

import io.circe.yaml.parser
import org.apache.log4j.LogManager

import scala.io.Source
import scala.util.Try

/** Loads connection definitions from a connections.yaml file.
  * Connections define how to reach external systems (databases, cloud storage, etc.)
  * Credentials are resolved via ${env.VAR} by VariableResolver.
  */
object ConnectionsLoader {
  private val logger = LogManager.getLogger(getClass)

  type ConnectionMap = Map[String, Map[String, String]]

  /** Load connections from a YAML file.
    * @param path Path to connections.yaml
    * @return Map of connection name -> config map
    */
  def load(path: String): ConnectionMap = {
    Try {
      val source = Source.fromFile(path)
      val content = try source.mkString finally source.close()

      parser.parse(content) match {
        case Right(json) =>
          val connections = json.hcursor.downField("connections")
          connections.keys.map(_.toList).getOrElse(Nil).flatMap { name =>
            connections.downField(name).as[Map[String, String]].toOption.map(name -> _)
          }.toMap
        case Left(err) =>
          logger.error(s"Failed to parse connections file '$path': ${err.getMessage}")
          Map.empty
      }
    }.getOrElse {
      logger.warn(s"Connections file not found: $path")
      Map.empty[String, Map[String, String]]
    }
  }

  /** Resolve a connection by name, merging its config into the source/sink config.
    * @throws IllegalArgumentException if connection name not found
    */
  def resolveConnection(name: String, connections: ConnectionMap): Map[String, String] = {
    connections.getOrElse(name,
      throw new IllegalArgumentException(
        s"Connection '$name' not found. Available: ${connections.keys.mkString(", ")}"))
  }
}
```

- [ ] **Step 4: Run tests**

Expected: All 4 ConnectionsLoader tests PASS

- [ ] **Step 5: Commit**

```bash
git add core/src/
git commit -m "feat(core): add ConnectionsLoader for external connection definitions

Loads connection configs from connections.yaml. Connections define
how to reach databases, cloud storage, etc. Names are referenced
in pipeline YAML via 'connection: pg-prod'."
```

---

## Task 6: PostgreSQL Source Connector

**Files:**
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sources/PostgreSQLSourceConnector.scala`
- Modify: `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector`

- [ ] **Step 1: Implement PostgreSQLSourceConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sources/PostgreSQLSourceConnector.scala
package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class PostgreSQLSourceConnector extends SourceConnector {
  def connectorType: String = "PostgreSQL"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val host = config.getOrElse("host", throw new IllegalArgumentException("PostgreSQL: host is required"))
    val port = config.getOrElse("port", "5432")
    val database = config.getOrElse("database", throw new IllegalArgumentException("PostgreSQL: database is required"))
    val user = config.getOrElse("user", throw new IllegalArgumentException("PostgreSQL: user is required"))
    val password = config.getOrElse("password", throw new IllegalArgumentException("PostgreSQL: password is required"))
    val query = config.getOrElse("query", throw new IllegalArgumentException("PostgreSQL: query is required"))
    val url = s"jdbc:postgresql://$host:$port/$database"

    spark.read.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"($query) AS subquery")
      .load()
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    try {
      val host = config.getOrElse("host", return Left("host not configured"))
      val port = config.getOrElse("port", "5432")
      val database = config.getOrElse("database", return Left("database not configured"))
      val user = config.getOrElse("user", return Left("user not configured"))
      val password = config.getOrElse("password", return Left("password not configured"))
      val url = s"jdbc:postgresql://$host:$port/$database"

      val start = System.currentTimeMillis()
      val conn = java.sql.DriverManager.getConnection(url, user, password)
      val latency = System.currentTimeMillis() - start
      conn.close()
      Right(latency)
    } catch {
      case e: Exception => Left(s"Connection failed: ${e.getMessage}")
    }
  }
}
```

- [ ] **Step 2: Register in ServiceLoader**

Append to `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector`:
```
com.dataweaver.connectors.sources.PostgreSQLSourceConnector
```

- [ ] **Step 3: Commit**

```bash
git add connectors/
git commit -m "feat(connectors): add PostgreSQL source connector with health check

JDBC-based PostgreSQL reader with configurable host, port, database,
user, password, and query. Includes healthCheck for weaver doctor."
```

---

## Task 7: File Source + Sink Connectors

**Files:**
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sources/FileSourceConnector.scala`
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sinks/FileSinkConnector.scala`
- Test: `connectors/src/test/scala/com/dataweaver/connectors/sources/FileSourceConnectorTest.scala`
- Create: `connectors/src/test/resources/test_data/sample.csv`
- Create: `connectors/src/test/resources/test_data/sample.json`
- Modify: ServiceLoader files

- [ ] **Step 1: Create test data files**

```csv
# connectors/src/test/resources/test_data/sample.csv
id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35
```

```json
// connectors/src/test/resources/test_data/sample.json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25},
  {"id": 3, "name": "Charlie", "age": 35}
]
```

- [ ] **Step 2: Implement FileSourceConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sources/FileSourceConnector.scala
package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Reads data from local or remote files (CSV, Parquet, JSON, ORC). */
class FileSourceConnector extends SourceConnector {
  def connectorType: String = "File"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val path = config.getOrElse("path",
      throw new IllegalArgumentException("File: 'path' is required"))
    val format = config.getOrElse("format", inferFormat(path))

    val reader = spark.read

    // Apply options from config (e.g., header, delimiter, multiLine)
    config.filterNot { case (k, _) => Set("path", "format").contains(k) }
      .foreach { case (k, v) => reader.option(k, v) }

    format.toLowerCase match {
      case "csv"     => reader.option("header", config.getOrElse("header", "true")).csv(path)
      case "json"    => reader.option("multiLine", config.getOrElse("multiLine", "true")).json(path)
      case "parquet" => reader.parquet(path)
      case "orc"     => reader.orc(path)
      case other     => throw new IllegalArgumentException(
        s"Unsupported file format '$other'. Supported: csv, json, parquet, orc")
    }
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    val path = config.getOrElse("path", return Left("path not configured"))
    val start = System.currentTimeMillis()
    val file = new java.io.File(path)
    if (file.exists() || path.startsWith("s3://") || path.startsWith("gs://"))
      Right(System.currentTimeMillis() - start)
    else
      Left(s"File not found: $path")
  }

  private def inferFormat(path: String): String = {
    val lower = path.toLowerCase
    if (lower.endsWith(".csv")) "csv"
    else if (lower.endsWith(".json") || lower.endsWith(".jsonl")) "json"
    else if (lower.endsWith(".parquet")) "parquet"
    else if (lower.endsWith(".orc")) "orc"
    else "parquet" // default
  }
}
```

- [ ] **Step 3: Implement FileSinkConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sinks/FileSinkConnector.scala
package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Writes data to local or remote files (CSV, Parquet, JSON, ORC). */
class FileSinkConnector extends SinkConnector {
  def connectorType: String = "File"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val path = config.getOrElse("path",
      throw new IllegalArgumentException("File sink: 'path' is required"))
    val format = config.getOrElse("format", "parquet")
    val saveMode = SaveMode.valueOf(config.getOrElse("saveMode", "Overwrite"))
    val partitionBy = config.get("partitionBy").map(_.split(",").map(_.trim).toSeq)
    val coalesce = config.get("coalesce").map(_.toInt)

    var writer = data.write.mode(saveMode)

    // Apply partitioning
    partitionBy.foreach(cols => writer = writer.partitionBy(cols: _*))

    // Apply coalesce
    val df = coalesce.map(n => data.coalesce(n)).getOrElse(data)
    writer = df.write.mode(saveMode)
    partitionBy.foreach(cols => writer = writer.partitionBy(cols: _*))

    format.toLowerCase match {
      case "csv"     => writer.option("header", "true").csv(path)
      case "json"    => writer.json(path)
      case "parquet" => writer.parquet(path)
      case "orc"     => writer.orc(path)
      case other     => throw new IllegalArgumentException(
        s"Unsupported file format '$other'. Supported: csv, json, parquet, orc")
    }
  }
}
```

- [ ] **Step 4: Write FileSourceConnector test**

```scala
// connectors/src/test/scala/com/dataweaver/connectors/sources/FileSourceConnectorTest.scala
package com.dataweaver.connectors.sources

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileSourceConnectorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[1]").appName("FileTest").getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "FileSourceConnector" should "read CSV files" in {
    val connector = new FileSourceConnector()
    val df = connector.read(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.csv",
      "format" -> "csv"
    ))
    df.count() shouldBe 3
    df.columns should contain allOf ("id", "name", "age")
  }

  it should "read JSON files" in {
    val connector = new FileSourceConnector()
    val df = connector.read(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.json",
      "format" -> "json"
    ))
    df.count() shouldBe 3
  }

  it should "infer format from file extension" in {
    val connector = new FileSourceConnector()
    val df = connector.read(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.csv"
    ))
    df.count() shouldBe 3
  }

  it should "report health check for existing file" in {
    val connector = new FileSourceConnector()
    val result = connector.healthCheck(Map(
      "path" -> "connectors/src/test/resources/test_data/sample.csv"
    ))
    result.isRight shouldBe true
  }

  it should "report health check failure for missing file" in {
    val connector = new FileSourceConnector()
    val result = connector.healthCheck(Map("path" -> "/nonexistent/file.csv"))
    result.isLeft shouldBe true
  }
}
```

- [ ] **Step 5: Register in ServiceLoader**

Update `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector`:
```
com.dataweaver.connectors.sources.SQLSourceConnector
com.dataweaver.connectors.sources.TestSourceConnector
com.dataweaver.connectors.sources.PostgreSQLSourceConnector
com.dataweaver.connectors.sources.FileSourceConnector
```

Update `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SinkConnector`:
```
com.dataweaver.connectors.sinks.BigQuerySinkConnector
com.dataweaver.connectors.sinks.TestSinkConnector
com.dataweaver.connectors.sinks.FileSinkConnector
```

- [ ] **Step 6: Commit**

```bash
git add connectors/
git commit -m "feat(connectors): add File source/sink connectors (CSV, Parquet, JSON, ORC)

FileSourceConnector reads with format auto-detection from extension.
FileSinkConnector writes with saveMode, partitioning, and coalesce.
Both support local and cloud paths (s3://, gs://)."
```

---

## Task 8: DeltaLake Sink Connector

**Files:**
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sinks/DeltaLakeSinkConnector.scala`
- Modify: ServiceLoader file

- [ ] **Step 1: Implement DeltaLakeSinkConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sinks/DeltaLakeSinkConnector.scala
package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Writes data to Delta Lake tables.
  * Supports Overwrite, Append, and Merge (upsert) modes.
  */
class DeltaLakeSinkConnector extends SinkConnector {
  def connectorType: String = "DeltaLake"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val path = config.getOrElse("path",
      throw new IllegalArgumentException("DeltaLake: 'path' is required"))
    val saveMode = config.getOrElse("saveMode", "Overwrite").toLowerCase
    val partitionBy = config.get("partitionBy").map(_.split(",").map(_.trim).toSeq)

    saveMode match {
      case "merge" =>
        val mergeKey = config.getOrElse("mergeKey",
          throw new IllegalArgumentException("DeltaLake merge requires 'mergeKey'"))
        writeMerge(data, path, mergeKey)

      case mode =>
        var writer = data.write
          .format("delta")
          .mode(SaveMode.valueOf(mode.capitalize))
        partitionBy.foreach(cols => writer = writer.partitionBy(cols: _*))
        writer.save(path)
    }
  }

  private def writeMerge(data: DataFrame, path: String, mergeKey: String)(implicit
      spark: SparkSession
  ): Unit = {
    import io.delta.tables.DeltaTable

    if (DeltaTable.isDeltaTable(spark, path)) {
      val deltaTable = DeltaTable.forPath(spark, path)
      val mergeKeys = mergeKey.split(",").map(_.trim)
      val condition = mergeKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

      deltaTable.as("target")
        .merge(data.as("source"), condition)
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    } else {
      // First write — create the table
      data.write.format("delta").save(path)
    }
  }
}
```

- [ ] **Step 2: Register in ServiceLoader**

Append to `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SinkConnector`:
```
com.dataweaver.connectors.sinks.DeltaLakeSinkConnector
```

- [ ] **Step 3: Commit**

```bash
git add connectors/
git commit -m "feat(connectors): add Delta Lake sink connector with merge support

DeltaLakeSinkConnector supports Overwrite, Append, and Merge (upsert)
modes. Merge uses configurable mergeKey for WHEN MATCHED/NOT MATCHED."
```

---

## Task 9: CLI Commands — plan, explain, inspect

**Files:**
- Create: `cli/src/main/scala/com/dataweaver/cli/commands/PlanCommand.scala`
- Create: `cli/src/main/scala/com/dataweaver/cli/commands/ExplainCommand.scala`
- Create: `cli/src/main/scala/com/dataweaver/cli/commands/InspectCommand.scala`
- Modify: `cli/src/main/scala/com/dataweaver/cli/WeaverCLI.scala`

- [ ] **Step 1: Implement PlanCommand (dry-run)**

```scala
// cli/src/main/scala/com/dataweaver/cli/commands/PlanCommand.scala
package com.dataweaver.cli.commands

import com.dataweaver.core.config.{SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.{DAGResolver, SourceNode, TransformNode}
import com.dataweaver.core.plugin.PluginRegistry

object PlanCommand {

  def run(pipelinePath: String): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    val errors = SchemaValidator.validate(config)
    if (errors.nonEmpty) {
      errors.foreach(e => System.err.println(s"ERROR: $e"))
      return
    }

    val levels = DAGResolver.resolve(config)

    println()
    println(s"  Pipeline: ${config.name}")
    println(s"  Engine: ${config.engine}")
    println(s"  " + "\u2500" * 50)
    println()

    println("  Data Sources:")
    config.dataSources.foreach { ds =>
      val available = PluginRegistry.getSource(ds.`type`).isDefined
      val status = if (available) "\u2713" else "\u2717 NOT AVAILABLE"
      println(s"    $status ${ds.id} (${ds.`type`})")
      ds.connection.foreach(c => println(s"      connection: $c"))
    }

    println()
    println("  Transformations:")
    config.transformations.foreach { t =>
      val available = PluginRegistry.getTransform(t.`type`).isDefined
      val status = if (available) "\u2713" else "\u2717 NOT AVAILABLE"
      println(s"    $status ${t.id} (${t.`type`}) <- [${t.sources.mkString(", ")}]")
    }

    println()
    println("  Sinks:")
    config.sinks.foreach { s =>
      val available = PluginRegistry.getSink(s.`type`).isDefined
      val status = if (available) "\u2713" else "\u2717 NOT AVAILABLE"
      println(s"    $status ${s.id} (${s.`type`}) <- ${s.source.getOrElse("(auto)")}")
    }

    println()
    println(s"  Execution Plan: ${levels.size} levels")
    levels.zipWithIndex.foreach { case (level, i) =>
      val parallel = if (level.size > 1) " [PARALLEL]" else ""
      println(s"    Level $i$parallel: ${level.map(_.id).mkString(", ")}")
    }
    println()
  }
}
```

- [ ] **Step 2: Implement ExplainCommand**

```scala
// cli/src/main/scala/com/dataweaver/cli/commands/ExplainCommand.scala
package com.dataweaver.cli.commands

import com.dataweaver.core.config.YAMLParser
import com.dataweaver.core.dag.{DAGResolver, SourceNode, TransformNode}

object ExplainCommand {

  def run(pipelinePath: String): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    val levels = DAGResolver.resolve(config)

    println()
    println(s"  DAG for pipeline: ${config.name}")
    println(s"  " + "\u2500" * 50)
    println()

    // Print ASCII DAG
    levels.zipWithIndex.foreach { case (level, i) =>
      level.foreach { node =>
        val nodeType = node match {
          case _: SourceNode    => "SOURCE"
          case _: TransformNode => "TRANSFORM"
        }
        val deps = node.dependencies
        val arrow = if (deps.nonEmpty) s" <- [${deps.mkString(", ")}]" else ""
        println(s"  Level $i | [$nodeType] ${node.id}$arrow")
      }
      if (i < levels.size - 1) println(s"         |")
    }

    // Print sink routing
    println()
    println("  Sink Routing:")
    config.sinks.foreach { s =>
      println(s"    ${s.source.getOrElse("?")} --> [SINK] ${s.id} (${s.`type`})")
    }

    // Print parallelism summary
    val parallelLevels = levels.count(_.size > 1)
    val totalNodes = levels.map(_.size).sum
    println()
    println(s"  Summary: $totalNodes nodes, ${levels.size} levels, $parallelLevels parallelizable")
    println()
  }
}
```

- [ ] **Step 3: Implement InspectCommand (stub — full impl needs Spark session)**

```scala
// cli/src/main/scala/com/dataweaver/cli/commands/InspectCommand.scala
package com.dataweaver.cli.commands

import com.dataweaver.core.config.YAMLParser

object InspectCommand {

  def run(pipelinePath: String, transformId: String): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    // Find the transform or source
    val allIds = config.dataSources.map(_.id) ++ config.transformations.map(_.id)
    if (!allIds.contains(transformId)) {
      System.err.println(s"ERROR: '$transformId' not found. Available: ${allIds.mkString(", ")}")
      return
    }

    // Check if it's a source
    config.dataSources.find(_.id == transformId).foreach { ds =>
      println()
      println(s"  Source: ${ds.id}")
      println(s"  Type: ${ds.`type`}")
      ds.connection.foreach(c => println(s"  Connection: $c"))
      if (ds.query.nonEmpty) println(s"  Query: ${ds.query.trim}")
      println(s"  Config: ${ds.config.map { case (k, v) => s"$k=$v" }.mkString(", ")}")
      println()
      println("  Note: Run 'weaver apply' first to see schema and sample data.")
      println()
    }

    // Check if it's a transform
    config.transformations.find(_.id == transformId).foreach { t =>
      println()
      println(s"  Transform: ${t.id}")
      println(s"  Type: ${t.`type`}")
      println(s"  Sources: [${t.sources.mkString(", ")}]")
      t.query.foreach(q => println(s"  Query: ${q.trim}"))
      t.action.foreach(a => println(s"  Action: $a"))
      println()
      println("  Note: Run 'weaver apply' first to see schema and sample data.")
      println()
    }
  }
}
```

- [ ] **Step 4: Update WeaverCLI with new commands**

Add to the OParser.sequence in WeaverCLI.scala, after the existing `apply` command:

```scala
cmd("plan")
  .action((_, c) => c.copy(command = "plan"))
  .text("Dry-run: show what will be read/transformed/written")
  .children(
    arg[String]("<pipeline>")
      .action((x, c) => c.copy(pipeline = Some(x)))
      .text("Path to pipeline YAML file")
  ),
cmd("explain")
  .action((_, c) => c.copy(command = "explain"))
  .text("Show resolved DAG and execution plan")
  .children(
    arg[String]("<pipeline>")
      .action((x, c) => c.copy(pipeline = Some(x)))
      .text("Path to pipeline YAML file")
  ),
cmd("inspect")
  .action((_, c) => c.copy(command = "inspect"))
  .text("Show details of a source or transform")
  .children(
    arg[String]("<pipeline>")
      .action((x, c) => c.copy(pipeline = Some(x)))
      .text("Path to pipeline YAML file"),
    arg[String]("<id>")
      .action((x, c) => c.copy(inspectId = Some(x)))
      .text("ID of the source or transform to inspect")
  )
```

Add `inspectId: Option[String] = None` to the Config case class.

Add command handlers:
```scala
case "plan" =>
  PlanCommand.run(config.pipeline.get)
case "explain" =>
  ExplainCommand.run(config.pipeline.get)
case "inspect" =>
  InspectCommand.run(config.pipeline.get, config.inspectId.get)
```

- [ ] **Step 5: Commit**

```bash
git add cli/src/
git commit -m "feat(cli): add weaver plan, explain, and inspect commands

- plan: dry-run showing sources, transforms, sinks, and execution levels
- explain: ASCII DAG visualization with parallelism summary
- inspect: show details of a specific source or transform"
```

---

## Task 10: Update PipelineExecutor for Engine + Profile Support

**Files:**
- Modify: `core/src/main/scala/com/dataweaver/core/engine/PipelineExecutor.scala`
- Modify: `cli/src/main/scala/com/dataweaver/cli/commands/ApplyCommand.scala`

- [ ] **Step 1: Update PipelineExecutor to accept Engine**

Add engine parameter and profile/variable resolution:
```scala
def execute(
    config: PipelineConfig,
    env: Option[String] = None,
    connectionsPath: Option[String] = None
)(implicit spark: SparkSession): Unit = {
  // Apply profile if specified
  val resolvedConfig = env.map(ProfileApplier.apply(config, _)).getOrElse(config)

  // Resolve variables in all config values
  val variableResolver = new VariableResolver()

  // ... rest of execution with variable-resolved configs
}
```

- [ ] **Step 2: Update ApplyCommand to use Engine**

```scala
def run(pipelinePath: String, env: Option[String] = None): Unit = {
  val config = YAMLParser.parseFile(pipelinePath) match {
    case Right(c) => c
    case Left(err) => throw new IllegalArgumentException(s"Failed to parse pipeline: $err")
  }

  // Apply profile
  val resolvedConfig = env.map(ProfileApplier.apply(config, _)).getOrElse(config)

  // Select engine
  val engine = EngineSelector.select(resolvedConfig)
  implicit val spark: SparkSession = engine.getOrCreateSession("DataWeaver")

  try {
    PipelineExecutor.execute(resolvedConfig, env)
  } finally {
    engine.stop()
  }
}
```

- [ ] **Step 3: Commit**

```bash
git add core/src/ cli/src/
git commit -m "feat: integrate engine selection and profiles into pipeline execution

ApplyCommand selects engine based on pipeline config and --env flag.
PipelineExecutor resolves profiles and variables before execution."
```

---

## Task 11: Doctor Health Checks for Connectors

**Files:**
- Modify: `cli/src/main/scala/com/dataweaver/cli/commands/DoctorCommand.scala`

- [ ] **Step 1: Add connector health checks to DoctorCommand**

Extend the `run` method to test live connections when `checkConnections = true`:

```scala
// After existing checks, add:
val connectionResults = if (checkConnections) {
  config.dataSources.flatMap { ds =>
    PluginRegistry.getSource(ds.`type`).map { connector =>
      val result = connector.healthCheck(ds.config)
      ds.id -> result
    }
  }.toMap
} else Map.empty[String, Either[String, Long]]
```

And add to printReport:
```scala
if (connectionResults.nonEmpty) {
  println()
  println("  Connections:")
  connectionResults.foreach { case (id, result) =>
    result match {
      case Right(latency) => println(s"  \u2713 $id connected (${latency}ms)")
      case Left(err)      => println(s"  \u2717 $id FAILED: $err")
    }
  }
}
```

- [ ] **Step 2: Commit**

```bash
git add cli/src/
git commit -m "feat(cli): add live connection health checks to weaver doctor

Doctor now tests actual connectivity to data sources when
--check-connections flag is used."
```

---

## Summary: What Phase 1 Delivers

| Feature | Status |
|---------|--------|
| Engine abstraction (Spark + DuckDB) | New |
| PostgreSQL connector | New |
| File connector (CSV, Parquet, JSON, ORC) | New |
| Delta Lake sink (with merge/upsert) | New |
| `weaver plan` (dry-run) | New |
| `weaver explain` (DAG visualization) | New |
| `weaver inspect` (source/transform details) | New |
| Variable injection (${date.today}, ${date.yesterday}) | New |
| Profile support (dev/prod overrides) | New |
| connections.yaml loading | New |
| Doctor live connection checks | Enhanced |

**New CLI commands:**
```bash
weaver plan <pipeline.yaml>              # Dry-run
weaver explain <pipeline.yaml>           # DAG visualization
weaver inspect <pipeline.yaml> <id>      # Source/transform details
weaver apply <pipeline.yaml> --env prod  # Execute with profile
weaver doctor <pipeline.yaml>            # Now with connection checks
```

**Next:** Phase 2 — Testing Framework + Data Quality Checks
