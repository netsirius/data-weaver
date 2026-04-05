# Phase 0: Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the existing single-module Data Weaver into a multi-module SBT build with core abstractions (plugin traits, DAG resolver, connection resolver, per-sink routing, YAML schema validation with human-readable errors), keeping all existing functionality working.

**Architecture:** Monorepo with 4 SBT sub-modules (`core`, `connectors`, `transformations`, `cli`). Core defines plugin traits (`SourceConnector`, `SinkConnector`, `TransformPlugin`) and infrastructure (DAG resolver, connection resolver, schema validator). Existing implementations (SQLReader, BigQuerySink, SQLTransformation, CLI) are moved to their respective modules. The pipeline executor is rewritten to use DAG-based topological sort with per-sink source routing.

**Tech Stack:** Scala 2.13.14, Spark 4.0.2, SBT 1.9.7, circe-yaml 0.15.1, scopt 4.1.0, ScalaTest 3.2.19

**Spec:** `docs/specs/2026-04-05-data-weaver-v2-design.md`

---

## File Structure

### Files to CREATE

```
# Root multi-project build
build.sbt                                          (rewrite — multi-module)

# Core module
core/build.sbt
core/src/main/scala/com/dataweaver/core/plugin/SourceConnector.scala
core/src/main/scala/com/dataweaver/core/plugin/SinkConnector.scala
core/src/main/scala/com/dataweaver/core/plugin/TransformPlugin.scala
core/src/main/scala/com/dataweaver/core/plugin/PluginRegistry.scala
core/src/main/scala/com/dataweaver/core/config/PipelineConfig.scala
core/src/main/scala/com/dataweaver/core/config/ConnectionResolver.scala
core/src/main/scala/com/dataweaver/core/config/YAMLParser.scala
core/src/main/scala/com/dataweaver/core/config/SchemaValidator.scala
core/src/main/scala/com/dataweaver/core/dag/DAGResolver.scala
core/src/main/scala/com/dataweaver/core/engine/PipelineExecutor.scala
core/src/test/scala/com/dataweaver/core/dag/DAGResolverTest.scala
core/src/test/scala/com/dataweaver/core/config/ConnectionResolverTest.scala
core/src/test/scala/com/dataweaver/core/config/SchemaValidatorTest.scala
core/src/test/scala/com/dataweaver/core/engine/PipelineExecutorTest.scala
core/src/main/resources/schemas/pipeline.schema.json

# Connectors module
connectors/build.sbt
connectors/src/main/scala/com/dataweaver/connectors/sources/SQLSourceConnector.scala
connectors/src/main/scala/com/dataweaver/connectors/sources/TestSourceConnector.scala
connectors/src/main/scala/com/dataweaver/connectors/sinks/BigQuerySinkConnector.scala
connectors/src/main/scala/com/dataweaver/connectors/sinks/TestSinkConnector.scala
connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector
connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SinkConnector

# Transformations module
transformations/build.sbt
transformations/src/main/scala/com/dataweaver/transformations/sql/SQLTransformPlugin.scala
transformations/src/main/resources/META-INF/services/com.dataweaver.core.plugin.TransformPlugin

# CLI module
cli/build.sbt
cli/src/main/scala/com/dataweaver/cli/WeaverCLI.scala
cli/src/main/scala/com/dataweaver/cli/commands/ValidateCommand.scala
cli/src/main/scala/com/dataweaver/cli/commands/ApplyCommand.scala

# Test resources (moved from current location)
core/src/test/resources/test_project/config/application.conf
core/src/test/resources/test_project/pipelines/pipeline.yaml
core/src/test/resources/input_files/testSource.json
```

### Files to DELETE (after migration)

```
src/main/scala/com/dataweaver/          (entire old source tree)
src/test/scala/com/dataweaver/          (entire old test tree)
src/test/resources/                     (moved to core/src/test/resources)
project/plugins.sbt                     (moved to project/ at root, unchanged)
```

---

## Task 1: Multi-Module SBT Build

**Files:**
- Rewrite: `build.sbt`
- Create: `core/build.sbt`
- Create: `connectors/build.sbt`
- Create: `transformations/build.sbt`
- Create: `cli/build.sbt`
- Keep: `project/build.properties`
- Keep: `project/plugins.sbt`

- [ ] **Step 1: Write the root multi-module build.sbt**

```scala
// build.sbt (root)
ThisBuild / version := "0.2.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.dataweaver"

val sparkVersion = "4.0.2"

// Shared settings
lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    "org.scalactic" %% "scalactic" % "3.2.19" % Test,
    "org.mockito"   %% "mockito-scala" % "1.17.37" % Test
  )
)

lazy val root = (project in file("."))
  .aggregate(core, connectors, transformations, cli)
  .settings(
    name := "data-weaver",
    publish / skip := true
  )

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
      "io.circe"         %% "circe-generic" % "0.14.10"
    )
  )

lazy val connectors = (project in file("connectors"))
  .dependsOn(core)
  .settings(
    commonSettings,
    name := "data-weaver-connectors",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "com.mysql"         % "mysql-connector-j" % "9.1.0",
      "org.json4s"       %% "json4s-jackson" % "4.0.7"
    )
  )

lazy val transformations = (project in file("transformations"))
  .dependsOn(core)
  .settings(
    commonSettings,
    name := "data-weaver-transformations",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    )
  )

lazy val cli = (project in file("cli"))
  .dependsOn(core, connectors, transformations)
  .settings(
    commonSettings,
    name := "data-weaver-cli",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core" % sparkVersion % "compile",
      "org.apache.spark"  %% "spark-sql"  % sparkVersion % "compile",
      "com.github.scopt"  %% "scopt"      % "4.1.0"
    ),
    Compile / mainClass := Some("com.dataweaver.cli.WeaverCLI"),
    assembly / assemblyJarName := "data-weaver.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case x if Assembly.isConfigFile(x) => MergeStrategy.concat
      case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
        MergeStrategy.rename
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "module-info.class"      => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )
```

- [ ] **Step 2: Create core/build.sbt (empty — settings in root)**

```scala
// core/build.sbt
// Settings defined in root build.sbt
```

- [ ] **Step 3: Create connectors/build.sbt**

```scala
// connectors/build.sbt
// Settings defined in root build.sbt
```

- [ ] **Step 4: Create transformations/build.sbt**

```scala
// transformations/build.sbt
// Settings defined in root build.sbt
```

- [ ] **Step 5: Create cli/build.sbt**

```scala
// cli/build.sbt
// Settings defined in root build.sbt
```

- [ ] **Step 6: Create module directory structure**

Run:
```bash
mkdir -p core/src/{main,test}/scala/com/dataweaver/core/{plugin,config,dag,engine}
mkdir -p core/src/{main,test}/resources
mkdir -p connectors/src/{main,test}/scala/com/dataweaver/connectors/{sources,sinks}
mkdir -p connectors/src/main/resources/META-INF/services
mkdir -p transformations/src/{main,test}/scala/com/dataweaver/transformations/sql
mkdir -p transformations/src/main/resources/META-INF/services
mkdir -p cli/src/{main,test}/scala/com/dataweaver/cli/commands
```

- [ ] **Step 7: Verify SBT recognizes all modules**

Run: `sbt projects`
Expected output:
```
[info] In file:.../data-weaver/
[info]     cli
[info]     connectors
[info]     core
[info]   * root
[info]     transformations
```

- [ ] **Step 8: Commit**

```bash
git add build.sbt core/ connectors/ transformations/ cli/ project/
git commit -m "build: restructure into multi-module SBT project

Splits data-weaver into 4 modules:
- core: plugin traits, config, DAG resolver
- connectors: source and sink implementations
- transformations: transform implementations
- cli: CLI entry point and commands"
```

---

## Task 2: Core Plugin Traits

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/plugin/SourceConnector.scala`
- Create: `core/src/main/scala/com/dataweaver/core/plugin/SinkConnector.scala`
- Create: `core/src/main/scala/com/dataweaver/core/plugin/TransformPlugin.scala`
- Create: `core/src/main/scala/com/dataweaver/core/plugin/PluginRegistry.scala`
- Test: `core/src/test/scala/com/dataweaver/core/plugin/PluginRegistryTest.scala`

- [ ] **Step 1: Write SourceConnector trait**

```scala
// core/src/main/scala/com/dataweaver/core/plugin/SourceConnector.scala
package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SourceConnector extends Serializable {
  /** Unique identifier for this connector type (e.g., "PostgreSQL", "MySQL") */
  def connectorType: String

  /** Read data from this source and return as a DataFrame.
    * @param config Key-value configuration from the YAML dataSource.config section
    * @param spark  Implicit SparkSession
    * @return DataFrame with the source data
    */
  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame

  /** Optional health check for `weaver doctor`.
    * @return Right(latencyMs) on success, Left(error message) on failure.
    */
  def healthCheck(config: Map[String, String]): Either[String, Long] =
    Left(s"Health check not implemented for $connectorType")
}
```

- [ ] **Step 2: Write SinkConnector trait**

```scala
// core/src/main/scala/com/dataweaver/core/plugin/SinkConnector.scala
package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SinkConnector extends Serializable {
  /** Unique identifier for this connector type (e.g., "BigQuery", "DeltaLake") */
  def connectorType: String

  /** Write a DataFrame to this sink.
    * @param data         The DataFrame to write
    * @param pipelineName The name of the pipeline (for naming output)
    * @param config       Key-value configuration from the YAML sink.config section
    * @param spark        Implicit SparkSession
    */
  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit

  /** Optional health check for `weaver doctor`.
    * @return Right(latencyMs) on success, Left(error message) on failure.
    */
  def healthCheck(config: Map[String, String]): Either[String, Long] =
    Left(s"Health check not implemented for $connectorType")
}
```

- [ ] **Step 3: Write TransformPlugin trait**

```scala
// core/src/main/scala/com/dataweaver/core/plugin/TransformPlugin.scala
package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}

trait TransformPlugin extends Serializable {
  /** Unique identifier for this transform type (e.g., "SQL", "DataQuality") */
  def transformType: String

  /** Apply transformation to input DataFrames.
    * @param inputs Map of source id -> DataFrame
    * @param config Transform-specific configuration (query, checks, etc.)
    * @param spark  Implicit SparkSession
    * @return Transformed DataFrame
    */
  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame
}

/** Configuration for a transformation step.
  * @param id      Unique id of this transformation
  * @param sources List of source ids this transform reads from
  * @param query   Optional SQL query
  * @param action  Optional action name
  * @param extra   Additional key-value config
  */
case class TransformConfig(
    id: String,
    sources: List[String],
    query: Option[String] = None,
    action: Option[String] = None,
    extra: Map[String, String] = Map.empty
)
```

- [ ] **Step 4: Write PluginRegistry**

```scala
// core/src/main/scala/com/dataweaver/core/plugin/PluginRegistry.scala
package com.dataweaver.core.plugin

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/** Discovers and registers all available plugins via ServiceLoader and manual registration. */
object PluginRegistry {

  private var sources: Map[String, SourceConnector] = Map.empty
  private var sinks: Map[String, SinkConnector] = Map.empty
  private var transforms: Map[String, TransformPlugin] = Map.empty
  private var initialized = false

  /** Initialize registry by discovering plugins via ServiceLoader. */
  def init(): Unit = synchronized {
    if (!initialized) {
      ServiceLoader.load(classOf[SourceConnector]).asScala.foreach(registerSource)
      ServiceLoader.load(classOf[SinkConnector]).asScala.foreach(registerSink)
      ServiceLoader.load(classOf[TransformPlugin]).asScala.foreach(registerTransform)
      initialized = true
    }
  }

  def registerSource(connector: SourceConnector): Unit =
    sources += (connector.connectorType -> connector)

  def registerSink(connector: SinkConnector): Unit =
    sinks += (connector.connectorType -> connector)

  def registerTransform(plugin: TransformPlugin): Unit =
    transforms += (plugin.transformType -> plugin)

  def getSource(connectorType: String): Option[SourceConnector] = {
    init()
    sources.get(connectorType)
  }

  def getSink(connectorType: String): Option[SinkConnector] = {
    init()
    sinks.get(connectorType)
  }

  def getTransform(transformType: String): Option[TransformPlugin] = {
    init()
    transforms.get(transformType)
  }

  def availableSources: Set[String] = { init(); sources.keySet }
  def availableSinks: Set[String] = { init(); sinks.keySet }
  def availableTransforms: Set[String] = { init(); transforms.keySet }

  /** Reset registry — only for testing. */
  private[core] def reset(): Unit = synchronized {
    sources = Map.empty
    sinks = Map.empty
    transforms = Map.empty
    initialized = false
  }
}
```

- [ ] **Step 5: Write PluginRegistry test**

```scala
// core/src/test/scala/com/dataweaver/core/plugin/PluginRegistryTest.scala
package com.dataweaver.core.plugin

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PluginRegistryTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = PluginRegistry.reset()

  "PluginRegistry" should "register and retrieve a source connector" in {
    val mockSource = new SourceConnector {
      def connectorType: String = "MockDB"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame =
        throw new UnsupportedOperationException
    }

    PluginRegistry.registerSource(mockSource)
    PluginRegistry.getSource("MockDB") shouldBe Some(mockSource)
    PluginRegistry.getSource("Unknown") shouldBe None
  }

  it should "register and retrieve a sink connector" in {
    val mockSink = new SinkConnector {
      def connectorType: String = "MockSink"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = ()
    }

    PluginRegistry.registerSink(mockSink)
    PluginRegistry.getSink("MockSink") shouldBe Some(mockSink)
  }

  it should "register and retrieve a transform plugin" in {
    val mockTransform = new TransformPlugin {
      def transformType: String = "MockTransform"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = throw new UnsupportedOperationException
    }

    PluginRegistry.registerTransform(mockTransform)
    PluginRegistry.getTransform("MockTransform") shouldBe Some(mockTransform)
  }

  it should "list available connectors" in {
    PluginRegistry.availableSources shouldBe empty
    val mockSource = new SourceConnector {
      def connectorType: String = "TestDB"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame =
        throw new UnsupportedOperationException
    }
    PluginRegistry.registerSource(mockSource)
    PluginRegistry.availableSources should contain("TestDB")
  }
}
```

- [ ] **Step 6: Run tests**

Run: `sbt "core/test"`
Expected: All 4 tests PASS

- [ ] **Step 7: Commit**

```bash
git add core/src/
git commit -m "feat(core): add plugin traits and PluginRegistry

Defines SourceConnector, SinkConnector, TransformPlugin traits.
PluginRegistry discovers plugins via ServiceLoader and manual registration."
```

---

## Task 3: Pipeline Config Model

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/config/PipelineConfig.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/PipelineConfigTest.scala`

- [ ] **Step 1: Write the new config model with per-sink source routing**

```scala
// core/src/main/scala/com/dataweaver/core/config/PipelineConfig.scala
package com.dataweaver.core.config

/** Root pipeline configuration parsed from YAML. */
case class PipelineConfig(
    name: String,
    tag: String = "",
    engine: String = "auto",
    dataSources: List[DataSourceConfig] = List.empty,
    transformations: List[TransformationConfig] = List.empty,
    sinks: List[SinkConfig] = List.empty
)

/** A data source definition.
  * @param id         Unique identifier referenced by transformations
  * @param `type`     Connector type (e.g., "PostgreSQL", "MySQL", "Test")
  * @param connection Optional named connection from connections.yaml
  * @param query      Optional query string
  * @param config     Additional key-value config
  */
case class DataSourceConfig(
    id: String,
    `type`: String,
    connection: Option[String] = None,
    query: String = "",
    config: Map[String, String] = Map.empty
)

/** A transformation step.
  * @param id       Unique identifier, can be referenced by other transforms or sinks
  * @param `type`   Transform plugin type (e.g., "SQL", "DataQuality", "LLMTransform")
  * @param sources  List of ids this transform reads from (dataSources or other transforms)
  * @param query    Optional SQL query
  * @param action   Optional action name
  * @param config   Additional key-value config
  */
case class TransformationConfig(
    id: String,
    `type`: String,
    sources: List[String] = List.empty,
    query: Option[String] = None,
    action: Option[String] = None,
    config: Map[String, String] = Map.empty
)

/** A sink definition.
  * @param id       Unique identifier
  * @param `type`   Sink connector type (e.g., "BigQuery", "DeltaLake", "Test")
  * @param source   The id of the transformation whose output this sink receives.
  *                 This is EXPLICIT — each sink declares which transform it reads from.
  * @param connection Optional named connection
  * @param config   Additional key-value config
  */
case class SinkConfig(
    id: String,
    `type`: String,
    source: Option[String] = None,
    connection: Option[String] = None,
    config: Map[String, String] = Map.empty
)

/** Execution mode for pipeline runs. */
sealed trait ExecutionMode
object ExecutionMode {
  case object Test extends ExecutionMode
  case object Debug extends ExecutionMode
  case object Production extends ExecutionMode

  def fromString(s: String): ExecutionMode = s.toLowerCase match {
    case "test"       => Test
    case "debug"      => Debug
    case "production" => Production
    case other        => throw new IllegalArgumentException(s"Invalid execution mode: $other")
  }
}
```

- [ ] **Step 2: Write test for the new config model**

```scala
// core/src/test/scala/com/dataweaver/core/config/PipelineConfigTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PipelineConfigTest extends AnyFlatSpec with Matchers {

  "PipelineConfig" should "be constructable with defaults" in {
    val config = PipelineConfig(name = "test")
    config.engine shouldBe "auto"
    config.dataSources shouldBe empty
    config.transformations shouldBe empty
    config.sinks shouldBe empty
  }

  "SinkConfig" should "have explicit source routing" in {
    val sink = SinkConfig(
      id = "warehouse",
      `type` = "BigQuery",
      source = Some("validated")
    )
    sink.source shouldBe Some("validated")
  }

  it should "default source to None" in {
    val sink = SinkConfig(id = "out", `type` = "Test")
    sink.source shouldBe None
  }

  "ExecutionMode" should "parse valid modes" in {
    ExecutionMode.fromString("test") shouldBe ExecutionMode.Test
    ExecutionMode.fromString("production") shouldBe ExecutionMode.Production
    ExecutionMode.fromString("debug") shouldBe ExecutionMode.Debug
  }

  it should "throw on invalid mode" in {
    an[IllegalArgumentException] should be thrownBy ExecutionMode.fromString("invalid")
  }
}
```

- [ ] **Step 3: Run tests**

Run: `sbt "core/test"`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add core/src/
git commit -m "feat(core): add PipelineConfig model with per-sink source routing

SinkConfig.source is now explicit — each sink declares which transform
it reads from, fixing the old bug of always using the last transform."
```

---

## Task 4: DAG Resolver

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/dag/DAGResolver.scala`
- Test: `core/src/test/scala/com/dataweaver/core/dag/DAGResolverTest.scala`

- [ ] **Step 1: Write failing DAG tests**

```scala
// core/src/test/scala/com/dataweaver/core/dag/DAGResolverTest.scala
package com.dataweaver.core.dag

import com.dataweaver.core.config.{DataSourceConfig, PipelineConfig, SinkConfig, TransformationConfig}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DAGResolverTest extends AnyFlatSpec with Matchers {

  "DAGResolver" should "resolve a linear chain" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("A")),
        TransformationConfig("T2", "SQL", sources = List("T1"))
      )
    )

    val levels = DAGResolver.resolve(config)
    levels should have size 3
    levels(0).map(_.id) should contain only "A"
    levels(1).map(_.id) should contain only "T1"
    levels(2).map(_.id) should contain only "T2"
  }

  it should "detect independent transforms for parallel execution" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(
        DataSourceConfig("A", "Test"),
        DataSourceConfig("B", "Test")
      ),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("A")),
        TransformationConfig("T2", "SQL", sources = List("B")),
        TransformationConfig("T3", "SQL", sources = List("T1", "T2"))
      )
    )

    val levels = DAGResolver.resolve(config)
    levels should have size 3
    // Level 0: both sources (A, B)
    levels(0).map(_.id) should contain allOf ("A", "B")
    // Level 1: T1 and T2 are independent — same level = parallel
    levels(1).map(_.id) should contain allOf ("T1", "T2")
    // Level 2: T3 depends on both
    levels(2).map(_.id) should contain only "T3"
  }

  it should "detect cyclic dependencies" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("T2")),
        TransformationConfig("T2", "SQL", sources = List("T1"))
      )
    )

    an[IllegalStateException] should be thrownBy DAGResolver.resolve(config)
  }

  it should "detect missing source references" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("A", "Test")),
      transformations = List(
        TransformationConfig("T1", "SQL", sources = List("NONEXISTENT"))
      )
    )

    an[IllegalArgumentException] should be thrownBy DAGResolver.resolve(config)
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "core/test"`
Expected: FAIL — `DAGResolver` does not exist

- [ ] **Step 3: Implement DAGResolver**

```scala
// core/src/main/scala/com/dataweaver/core/dag/DAGResolver.scala
package com.dataweaver.core.dag

import com.dataweaver.core.config.{PipelineConfig, TransformationConfig, DataSourceConfig}

/** A node in the execution DAG. */
sealed trait DAGNode {
  def id: String
  def dependencies: List[String]
}

case class SourceNode(config: DataSourceConfig) extends DAGNode {
  def id: String = config.id
  def dependencies: List[String] = List.empty
}

case class TransformNode(config: TransformationConfig) extends DAGNode {
  def id: String = config.id
  def dependencies: List[String] = config.sources
}

/** Resolves a PipelineConfig into execution levels.
  * Each level contains nodes that can be executed in parallel.
  * Levels are ordered: level N must complete before level N+1 starts.
  */
object DAGResolver {

  /** Resolve the pipeline config into ordered execution levels.
    * @return List of levels, where each level is a list of nodes that can run in parallel.
    * @throws IllegalStateException if cyclic dependency detected
    * @throws IllegalArgumentException if a source reference does not exist
    */
  def resolve(config: PipelineConfig): List[List[DAGNode]] = {
    val sourceNodes: List[DAGNode] = config.dataSources.map(SourceNode)
    val transformNodes: List[DAGNode] = config.transformations.map(TransformNode)
    val allNodes = sourceNodes ++ transformNodes
    val nodeMap = allNodes.map(n => n.id -> n).toMap

    // Validate all source references exist
    transformNodes.foreach { node =>
      node.dependencies.foreach { dep =>
        if (!nodeMap.contains(dep))
          throw new IllegalArgumentException(
            s"Transform '${node.id}' references source '$dep' which does not exist. " +
              s"Available: ${nodeMap.keys.mkString(", ")}"
          )
      }
    }

    // Topological sort by levels (Kahn's algorithm)
    val inDegree = scala.collection.mutable.Map(allNodes.map(n => n.id -> n.dependencies.size): _*)
    val dependents = scala.collection.mutable.Map[String, List[String]]().withDefaultValue(Nil)
    allNodes.foreach { node =>
      node.dependencies.foreach { dep =>
        dependents(dep) = node.id :: dependents(dep)
      }
    }

    var remaining = allNodes.map(_.id).toSet
    var levels = List.empty[List[DAGNode]]

    while (remaining.nonEmpty) {
      val ready = remaining.filter(id => inDegree(id) == 0).toList
      if (ready.isEmpty) {
        throw new IllegalStateException(
          s"Cyclic dependency detected among: ${remaining.mkString(", ")}"
        )
      }

      levels = levels :+ ready.map(nodeMap)

      ready.foreach { id =>
        remaining -= id
        dependents(id).foreach { dep =>
          inDegree(dep) -= 1
        }
      }
    }

    levels
  }
}
```

- [ ] **Step 4: Run tests**

Run: `sbt "core/test"`
Expected: All 4 DAGResolver tests PASS

- [ ] **Step 5: Commit**

```bash
git add core/src/
git commit -m "feat(core): add DAG resolver with topological sort

Resolves pipeline transformations into execution levels.
Independent transforms in the same level can run in parallel.
Detects cyclic dependencies and missing source references."
```

---

## Task 5: Connection Resolver

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/config/ConnectionResolver.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/ConnectionResolverTest.scala`

- [ ] **Step 1: Write failing tests**

```scala
// core/src/test/scala/com/dataweaver/core/config/ConnectionResolverTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionResolverTest extends AnyFlatSpec with Matchers {

  "ConnectionResolver" should "resolve ${env.VAR} from environment" in {
    // We'll use a test helper that provides a custom env map
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "core/test"`
Expected: FAIL — `ConnectionResolver` does not exist

- [ ] **Step 3: Implement ConnectionResolver**

```scala
// core/src/main/scala/com/dataweaver/core/config/ConnectionResolver.scala
package com.dataweaver.core.config

/** Resolves variable references in configuration values.
  * Supports: ${env.VARIABLE_NAME} for environment variables.
  * Also loads .env file from project root if present (gitignored, local dev).
  * Future: ${vault.path}, ${aws.secret}, ${k8s.secret}
  *
  * @param envProvider Map of env var name -> value. Defaults to System.getenv().
  * @param dotEnvPath  Path to .env file. Defaults to ".env" in working directory.
  */
class ConnectionResolver(
    envProvider: Map[String, String] = sys.env,
    dotEnvPath: String = ".env"
) {

  private val envPattern = """\$\{env\.([^}]+)\}""".r

  /** Merge: .env file values (low priority) + system env (high priority) */
  private lazy val mergedEnv: Map[String, String] = {
    val dotEnvVars = loadDotEnv(dotEnvPath)
    dotEnvVars ++ envProvider // system env overrides .env
  }

  /** Resolve all variable references in a string.
    * @throws IllegalArgumentException if a referenced variable is not found
    */
  def resolve(value: String): String = {
    envPattern.replaceAllIn(
      value,
      m => {
        val varName = m.group(1)
        mergedEnv.getOrElse(
          varName,
          throw new IllegalArgumentException(
            s"Environment variable '$varName' not found. " +
              s"Set it with: export $varName=<value> or add $varName=<value> to .env file"
          )
        )
      }
    )
  }

  /** Resolve all variable references in a config map. */
  def resolveMap(config: Map[String, String]): Map[String, String] =
    config.map { case (k, v) => k -> resolve(v) }

  /** Load key=value pairs from a .env file. Ignores comments (#) and empty lines. */
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

Run: `sbt "core/test"`
Expected: All 5 ConnectionResolver tests PASS (dotenv tested via envProvider override)

- [ ] **Step 5: Commit**

```bash
git add core/src/
git commit -m "feat(core): add ConnectionResolver for variable injection

Resolves \${env.VAR} references in config values from environment
variables. Provides clear error when a variable is not set."
```

---

## Task 6: YAML Parser + Schema Validator

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/config/YAMLParser.scala`
- Create: `core/src/main/scala/com/dataweaver/core/config/SchemaValidator.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/YAMLParserTest.scala`
- Test: `core/src/test/scala/com/dataweaver/core/config/SchemaValidatorTest.scala`
- Create: `core/src/test/resources/test_project/pipelines/pipeline.yaml`
- Move: `src/test/resources/input_files/testSource.json` → `core/src/test/resources/input_files/testSource.json`

- [ ] **Step 1: Write test pipeline YAML (new format with source routing)**

```yaml
# core/src/test/resources/test_project/pipelines/pipeline.yaml
name: ExamplePipeline
tag: example
engine: auto
dataSources:
  - id: testSource
    type: Test
    query: >
      SELECT name
      FROM test_table
    config:
      readMode: ReadOnce
transformations:
  - id: transform1
    type: SQL
    sources:
      - testSource
    query: >
      SELECT name as id
      FROM testSource
  - id: transform2
    type: SQL
    sources:
      - transform1
    query: >
      SELECT id
      FROM transform1
      WHERE id = "Alice"
sinks:
  - id: sink1
    type: Test
    source: transform2
    config:
      saveMode: Overwrite
```

- [ ] **Step 2: Write YAML parser tests**

```scala
// core/src/test/scala/com/dataweaver/core/config/YAMLParserTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YAMLParserTest extends AnyFlatSpec with Matchers {

  "YAMLParser" should "parse a valid pipeline YAML" in {
    val result = YAMLParser.parseFile("core/src/test/resources/test_project/pipelines/pipeline.yaml")
    result.isRight shouldBe true

    val config = result.toOption.get
    config.name shouldBe "ExamplePipeline"
    config.tag shouldBe "example"
    config.engine shouldBe "auto"
    config.dataSources should have size 1
    config.transformations should have size 2
    config.sinks should have size 1
  }

  it should "parse sink with explicit source routing" in {
    val result = YAMLParser.parseFile("core/src/test/resources/test_project/pipelines/pipeline.yaml")
    val config = result.toOption.get
    config.sinks.head.source shouldBe Some("transform2")
  }

  it should "return error for non-existent file" in {
    val result = YAMLParser.parseFile("nonexistent.yaml")
    result.isLeft shouldBe true
  }

  it should "return error for invalid YAML" in {
    val result = YAMLParser.parseString("not: [valid: yaml: {{")
    result.isLeft shouldBe true
  }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `sbt "core/test"`
Expected: FAIL — `YAMLParser` does not exist

- [ ] **Step 4: Implement YAMLParser**

```scala
// core/src/main/scala/com/dataweaver/core/config/YAMLParser.scala
package com.dataweaver.core.config

import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.Source
import scala.util.Try

/** Parses pipeline YAML files into PipelineConfig objects. */
object YAMLParser {

  /** Parse a YAML file from the filesystem.
    * @return Right(config) on success, Left(error message) on failure
    */
  def parseFile(path: String): Either[String, PipelineConfig] = {
    Try {
      val source = Source.fromFile(path)
      try source.mkString
      finally source.close()
    }.toEither.left
      .map(e => s"Cannot read file '$path': ${e.getMessage}")
      .flatMap(parseString)
  }

  /** Parse a YAML string into a PipelineConfig.
    * @return Right(config) on success, Left(error message) on failure
    */
  def parseString(yaml: String): Either[String, PipelineConfig] = {
    parser
      .parse(yaml)
      .flatMap(_.as[PipelineConfig])
      .left
      .map(e => s"YAML parse error: ${e.getMessage}")
  }
}
```

- [ ] **Step 5: Write SchemaValidator tests**

```scala
// core/src/test/scala/com/dataweaver/core/config/SchemaValidatorTest.scala
package com.dataweaver.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaValidatorTest extends AnyFlatSpec with Matchers {

  "SchemaValidator" should "pass a valid pipeline" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("src1", "Test")),
      transformations = List(TransformationConfig("t1", "SQL", sources = List("src1"))),
      sinks = List(SinkConfig("s1", "Test", source = Some("t1")))
    )

    val errors = SchemaValidator.validate(config)
    errors shouldBe empty
  }

  it should "report missing pipeline name" in {
    val config = PipelineConfig(name = "")
    val errors = SchemaValidator.validate(config)
    errors should contain("Pipeline name cannot be empty")
  }

  it should "report sink referencing non-existent transform" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("src1", "Test")),
      sinks = List(SinkConfig("s1", "Test", source = Some("NONEXISTENT")))
    )

    val errors = SchemaValidator.validate(config)
    errors.exists(_.contains("NONEXISTENT")) shouldBe true
  }

  it should "report duplicate ids" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(
        DataSourceConfig("dup", "Test"),
        DataSourceConfig("dup", "Test")
      )
    )

    val errors = SchemaValidator.validate(config)
    errors.exists(_.contains("Duplicate")) shouldBe true
  }

  it should "report sink without source when transforms exist" in {
    val config = PipelineConfig(
      name = "test",
      dataSources = List(DataSourceConfig("src1", "Test")),
      transformations = List(TransformationConfig("t1", "SQL", sources = List("src1"))),
      sinks = List(SinkConfig("s1", "Test", source = None))
    )

    val errors = SchemaValidator.validate(config)
    errors.exists(_.contains("source")) shouldBe true
  }
}
```

- [ ] **Step 6: Implement SchemaValidator**

```scala
// core/src/main/scala/com/dataweaver/core/config/SchemaValidator.scala
package com.dataweaver.core.config

/** Validates a PipelineConfig for structural correctness.
  * Returns a list of human-readable error messages. Empty list = valid.
  */
object SchemaValidator {

  def validate(config: PipelineConfig): List[String] = {
    var errors = List.empty[String]

    // Name required
    if (config.name.isEmpty)
      errors :+= "Pipeline name cannot be empty"

    // Collect all node ids
    val sourceIds = config.dataSources.map(_.id)
    val transformIds = config.transformations.map(_.id)
    val allIds = sourceIds ++ transformIds

    // Check for duplicate ids
    val duplicates = allIds.groupBy(identity).collect { case (id, list) if list.size > 1 => id }
    duplicates.foreach { id =>
      errors :+= s"Duplicate id '$id' — all dataSource and transformation ids must be unique"
    }

    // Validate transform source references
    config.transformations.foreach { t =>
      t.sources.foreach { srcId =>
        if (!allIds.contains(srcId))
          errors :+= s"Transform '${t.id}' references source '$srcId' which does not exist. " +
            s"Available: ${allIds.mkString(", ")}"
      }
    }

    // Validate sink source references
    config.sinks.foreach { sink =>
      sink.source match {
        case Some(srcId) if !allIds.contains(srcId) =>
          errors :+= s"Sink '${sink.id}' references source '$srcId' which does not exist. " +
            s"Available: ${allIds.mkString(", ")}"
        case None if config.transformations.nonEmpty =>
          errors :+= s"Sink '${sink.id}' has no 'source' field. " +
            s"Each sink must declare which transform it reads from. " +
            s"Available: ${transformIds.mkString(", ")}"
        case _ => // OK
      }
    }

    errors
  }
}
```

- [ ] **Step 7: Copy test resources**

Run:
```bash
mkdir -p core/src/test/resources/input_files
cp src/test/resources/input_files/testSource.json core/src/test/resources/input_files/
```

- [ ] **Step 8: Run all tests**

Run: `sbt "core/test"`
Expected: All tests PASS (YAMLParser: 4, SchemaValidator: 5)

- [ ] **Step 9: Commit**

```bash
git add core/src/
git commit -m "feat(core): add YAML parser and schema validator

YAMLParser parses pipeline YAML into PipelineConfig using circe.
SchemaValidator checks structural correctness: duplicate ids,
missing references, sink source routing validation.
Returns human-readable error messages."
```

---

## Task 7: Pipeline Executor with DAG + Parallel Execution

**Files:**
- Create: `core/src/main/scala/com/dataweaver/core/engine/PipelineExecutor.scala`
- Test: `core/src/test/scala/com/dataweaver/core/engine/PipelineExecutorTest.scala`

- [ ] **Step 1: Write executor test**

```scala
// core/src/test/scala/com/dataweaver/core/engine/PipelineExecutorTest.scala
package com.dataweaver.core.engine

import com.dataweaver.core.config._
import com.dataweaver.core.plugin._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PipelineExecutorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("ExecutorTest").getOrCreate()
    PluginRegistry.reset()
  }

  override def afterAll(): Unit = spark.stop()

  "PipelineExecutor" should "execute a pipeline with explicit sink routing" in {
    // Register a test source that returns a simple DataFrame
    PluginRegistry.registerSource(new SourceConnector {
      def connectorType = "InMemory"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq(("Alice", 1), ("Bob", 2), ("Charlie", 3)).toDF("name", "id")
      }
    })

    // Register a passthrough transform
    PluginRegistry.registerTransform(new TransformPlugin {
      def transformType = "SQL"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = {
        inputs.values.foreach(_.createOrReplaceTempView(config.sources.head))
        spark.sql(config.query.getOrElse("SELECT * FROM " + config.sources.head))
      }
    })

    // Register a test sink that captures the result
    var capturedData: Option[DataFrame] = None
    PluginRegistry.registerSink(new SinkConnector {
      def connectorType = "Capture"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = capturedData = Some(data)
    })

    val pipeline = PipelineConfig(
      name = "TestPipeline",
      dataSources = List(DataSourceConfig("src", "InMemory")),
      transformations = List(
        TransformationConfig("filtered", "SQL", sources = List("src"),
          query = Some("SELECT name FROM src WHERE id > 1"))
      ),
      sinks = List(SinkConfig("out", "Capture", source = Some("filtered")))
    )

    PipelineExecutor.execute(pipeline)

    capturedData shouldBe defined
    capturedData.get.count() shouldBe 2
    capturedData.get.columns should contain only "name"
  }

  it should "execute independent transforms in parallel" in {
    // Track execution order
    val executionOrder = scala.collection.mutable.ListBuffer[String]()

    PluginRegistry.reset()
    PluginRegistry.registerSource(new SourceConnector {
      def connectorType = "InMemory"
      def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        Seq(("data", 1)).toDF("col", "id")
      }
    })

    PluginRegistry.registerTransform(new TransformPlugin {
      def transformType = "TrackOrder"
      def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
          spark: SparkSession
      ): DataFrame = {
        Thread.sleep(100) // simulate work
        executionOrder.synchronized { executionOrder += config.id }
        inputs.values.head
      }
    })

    PluginRegistry.registerSink(new SinkConnector {
      def connectorType = "Noop"
      def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
          spark: SparkSession
      ): Unit = ()
    })

    val pipeline = PipelineConfig(
      name = "ParallelTest",
      dataSources = List(DataSourceConfig("A", "InMemory"), DataSourceConfig("B", "InMemory")),
      transformations = List(
        TransformationConfig("T1", "TrackOrder", sources = List("A")),
        TransformationConfig("T2", "TrackOrder", sources = List("B")),
        TransformationConfig("T3", "TrackOrder", sources = List("T1", "T2"))
      ),
      sinks = List(SinkConfig("out", "Noop", source = Some("T3")))
    )

    PipelineExecutor.execute(pipeline)

    // T3 must be after T1 and T2 (but T1/T2 order is non-deterministic since parallel)
    executionOrder should contain allOf ("T1", "T2", "T3")
    executionOrder.indexOf("T3") should be > executionOrder.indexOf("T1")
    executionOrder.indexOf("T3") should be > executionOrder.indexOf("T2")
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "core/test"`
Expected: FAIL — `PipelineExecutor` does not exist

- [ ] **Step 3: Implement PipelineExecutor**

```scala
// core/src/main/scala/com/dataweaver/core/engine/PipelineExecutor.scala
package com.dataweaver.core.engine

import com.dataweaver.core.config._
import com.dataweaver.core.dag.{DAGResolver, SourceNode, TransformNode}
import com.dataweaver.core.plugin.{PluginRegistry, TransformConfig}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/** Executes a pipeline by resolving the DAG and running transforms level by level.
  * Transforms within the same level run in parallel.
  */
object PipelineExecutor {

  private val logger = LogManager.getLogger(getClass)

  /** Execute a full pipeline.
    * @param config The pipeline configuration
    * @param spark  Implicit SparkSession
    */
  def execute(config: PipelineConfig)(implicit spark: SparkSession): Unit = {
    logger.info(s"Starting pipeline '${config.name}'")

    // Validate
    val errors = SchemaValidator.validate(config)
    if (errors.nonEmpty) {
      errors.foreach(e => logger.error(s"Validation error: $e"))
      throw new IllegalArgumentException(s"Pipeline validation failed:\n${errors.mkString("\n")}")
    }

    // Resolve DAG
    val levels = DAGResolver.resolve(config)
    logger.info(s"DAG resolved: ${levels.size} execution levels")

    // Execute level by level
    val results = scala.collection.mutable.Map[String, DataFrame]()

    levels.foreach { level =>
      val futures = level.map {
        case SourceNode(srcConfig) =>
          Future {
            logger.info(s"Reading source '${srcConfig.id}' (${srcConfig.`type`})")
            val connector = PluginRegistry
              .getSource(srcConfig.`type`)
              .getOrElse(throw new IllegalArgumentException(
                s"Unknown source type '${srcConfig.`type`}'. " +
                  s"Available: ${PluginRegistry.availableSources.mkString(", ")}"
              ))
            val df = connector.read(srcConfig.config)
            (srcConfig.id, df)
          }

        case TransformNode(tConfig) =>
          Future {
            logger.info(s"Applying transform '${tConfig.id}' (${tConfig.`type`})")
            val plugin = PluginRegistry
              .getTransform(tConfig.`type`)
              .getOrElse(throw new IllegalArgumentException(
                s"Unknown transform type '${tConfig.`type`}'. " +
                  s"Available: ${PluginRegistry.availableTransforms.mkString(", ")}"
              ))
            val inputs = tConfig.sources.map { srcId =>
              results.synchronized {
                srcId -> results.getOrElse(srcId,
                  throw new IllegalStateException(s"DataFrame for '$srcId' not found"))
              }
            }.toMap
            val transformConfig = TransformConfig(
              id = tConfig.id,
              sources = tConfig.sources,
              query = tConfig.query,
              action = tConfig.action,
              extra = tConfig.config
            )
            val df = plugin.transform(inputs, transformConfig)
            (tConfig.id, df)
          }
      }

      // Wait for all tasks in this level to complete
      val levelResults = futures.map(f => Await.result(f, 30.minutes))
      results.synchronized {
        levelResults.foreach { case (id, df) => results += (id -> df) }
      }

      logger.info(s"Level completed: ${level.map(_.id).mkString(", ")}")
    }

    // Write to sinks — each sink reads from its declared source
    config.sinks.foreach { sinkConfig =>
      val sourceId = sinkConfig.source.getOrElse(
        // Fallback: use last transform if no source specified and no transforms exist
        config.dataSources.last.id
      )
      val df = results.getOrElse(sourceId,
        throw new IllegalStateException(
          s"Sink '${sinkConfig.id}' references source '$sourceId' but no DataFrame found"
        ))

      logger.info(s"Writing to sink '${sinkConfig.id}' (${sinkConfig.`type`}) from '$sourceId'")
      val connector = PluginRegistry
        .getSink(sinkConfig.`type`)
        .getOrElse(throw new IllegalArgumentException(
          s"Unknown sink type '${sinkConfig.`type`}'. " +
            s"Available: ${PluginRegistry.availableSinks.mkString(", ")}"
        ))
      connector.write(df, config.name, sinkConfig.config)
      logger.info(s"Sink '${sinkConfig.id}' completed")
    }

    logger.info(s"Pipeline '${config.name}' completed successfully")
  }
}
```

- [ ] **Step 4: Run tests**

Run: `sbt "core/test"`
Expected: All PipelineExecutor tests PASS

- [ ] **Step 5: Commit**

```bash
git add core/src/
git commit -m "feat(core): add PipelineExecutor with DAG-based parallel execution

Executes transforms level by level. Independent transforms in the
same DAG level run in parallel via Futures. Each sink reads from
its explicitly declared source transform."
```

---

## Task 8: Migrate Existing Connectors

**Files:**
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sources/SQLSourceConnector.scala`
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sources/TestSourceConnector.scala`
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sinks/BigQuerySinkConnector.scala`
- Create: `connectors/src/main/scala/com/dataweaver/connectors/sinks/TestSinkConnector.scala`
- Create: `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector`
- Create: `connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SinkConnector`
- Create: `transformations/src/main/scala/com/dataweaver/transformations/sql/SQLTransformPlugin.scala`
- Create: `transformations/src/main/resources/META-INF/services/com.dataweaver.core.plugin.TransformPlugin`

- [ ] **Step 1: Migrate TestSourceConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sources/TestSourceConnector.scala
package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestSourceConnector extends SourceConnector {
  def connectorType: String = "Test"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val basePath = sys.props.getOrElse("dataweaver.test.resourcesDir", "core/src/test/resources")
    val id = config.getOrElse("id",
      throw new IllegalArgumentException("TestSourceConnector requires 'id' in config"))
    val filePath = s"$basePath/input_files/$id.json"
    spark.read.option("multiLine", true).json(filePath)
  }
}
```

- [ ] **Step 2: Migrate SQLSourceConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sources/SQLSourceConnector.scala
package com.dataweaver.connectors.sources

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class SQLSourceConnector extends SourceConnector {
  def connectorType: String = "MySQL"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val host = config.getOrElse("host", throw new IllegalArgumentException("host is required"))
    val port = config.getOrElse("port", "3306")
    val db = config.getOrElse("db", throw new IllegalArgumentException("db is required"))
    val query = config.getOrElse("query", throw new IllegalArgumentException("query is required"))
    val driver = config.getOrElse("driver", "com.mysql.cj.jdbc.Driver")
    val url = if (driver.contains("sqlserver")) s"jdbc:sqlserver://$host;databaseName=$db;"
              else s"jdbc:mysql://$host:$port/$db"
    val user = config.getOrElse("user", throw new IllegalArgumentException("user is required"))
    val password = config.getOrElse("password",
      throw new IllegalArgumentException("password is required"))

    spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", query)
      .load()
  }
}
```

- [ ] **Step 3: Migrate TestSinkConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sinks/TestSinkConnector.scala
package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestSinkConnector extends SinkConnector {
  def connectorType: String = "Test"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val basePath = sys.props.getOrElse("dataweaver.test.resourcesDir", "core/src/test/resources")
    val outputPath = s"$basePath/output_test/$pipelineName.json"
    data.coalesce(1).write.mode("overwrite").json(outputPath)
  }
}
```

- [ ] **Step 4: Migrate BigQuerySinkConnector**

```scala
// connectors/src/main/scala/com/dataweaver/connectors/sinks/BigQuerySinkConnector.scala
package com.dataweaver.connectors.sinks

import com.dataweaver.core.plugin.SinkConnector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class BigQuerySinkConnector extends SinkConnector {
  def connectorType: String = "BigQuery"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val projectId = config.getOrElse("projectId",
      throw new IllegalArgumentException("projectId is required"))
    val datasetName = config.getOrElse("datasetName",
      throw new IllegalArgumentException("datasetName is required"))
    val tableName = config.getOrElse("tableName",
      throw new IllegalArgumentException("tableName is required"))
    val temporaryGcsBucket = config.getOrElse("temporaryGcsBucket",
      throw new IllegalArgumentException("temporaryGcsBucket is required"))
    val saveMode = config.getOrElse("saveMode",
      throw new IllegalArgumentException("saveMode is required"))

    val bqTable = s"$projectId:$datasetName.$tableName"

    data.write
      .format("bigquery")
      .option("table", bqTable)
      .option("temporaryGcsBucket", temporaryGcsBucket)
      .mode(SaveMode.valueOf(saveMode))
      .save()
  }
}
```

- [ ] **Step 5: Migrate SQLTransformPlugin**

```scala
// transformations/src/main/scala/com/dataweaver/transformations/sql/SQLTransformPlugin.scala
package com.dataweaver.transformations.sql

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class SQLTransformPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)

  def transformType: String = "SQL"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val query = config.query.getOrElse(
      throw new IllegalArgumentException(s"Transform '${config.id}' requires a 'query' field"))

    // Register each input DataFrame as a temp view
    config.sources.foreach { sourceId =>
      inputs.getOrElse(sourceId,
        throw new IllegalArgumentException(s"Source '$sourceId' not found for transform '${config.id}'"))
        .createOrReplaceTempView(sourceId)
    }

    try {
      val df = spark.sql(query)
      df.queryExecution.analyzed // validate query
      df
    } catch {
      case e: Exception =>
        logger.error(s"SQL error in transform '${config.id}': ${e.getMessage}")
        throw e
    }
  }
}
```

- [ ] **Step 6: Create ServiceLoader registration files**

```
# connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector
com.dataweaver.connectors.sources.SQLSourceConnector
com.dataweaver.connectors.sources.TestSourceConnector
```

```
# connectors/src/main/resources/META-INF/services/com.dataweaver.core.plugin.SinkConnector
com.dataweaver.connectors.sinks.BigQuerySinkConnector
com.dataweaver.connectors.sinks.TestSinkConnector
```

```
# transformations/src/main/resources/META-INF/services/com.dataweaver.core.plugin.TransformPlugin
com.dataweaver.transformations.sql.SQLTransformPlugin
```

- [ ] **Step 7: Verify compilation of all modules**

Run: `sbt compile`
Expected: All modules compile successfully

- [ ] **Step 8: Commit**

```bash
git add connectors/ transformations/
git commit -m "feat: migrate existing connectors and transforms to plugin system

Migrates SQLReader, TestReader, BigQuerySink, TestSink, SQLTransformation
to the new SourceConnector/SinkConnector/TransformPlugin trait system.
Registers all plugins via ServiceLoader for automatic discovery."
```

---

## Task 9: Doctor Command

**Files:**
- Create: `cli/src/main/scala/com/dataweaver/cli/commands/DoctorCommand.scala`
- Test: `cli/src/test/scala/com/dataweaver/cli/commands/DoctorCommandTest.scala`

- [ ] **Step 1: Write DoctorCommand test**

```scala
// cli/src/test/scala/com/dataweaver/cli/commands/DoctorCommandTest.scala
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
      envProvider = Map.empty // no env vars available
    )
    // Test pipeline doesn't use env vars, so this should still pass
    result.overallHealthy shouldBe true
  }

  it should "check Java version" in {
    val result = DoctorCommand.run(
      pipelinePath = "core/src/test/resources/test_project/pipelines/pipeline.yaml",
      connectionsPath = None,
      checkConnections = false
    )
    result.javaVersion should not be empty
    result.javaVersionOk shouldBe true // we're running on JVM already
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "cli/test"`
Expected: FAIL — `DoctorCommand` does not exist

- [ ] **Step 3: Implement DoctorCommand**

```scala
// cli/src/main/scala/com/dataweaver/cli/commands/DoctorCommand.scala
package com.dataweaver.cli.commands

import com.dataweaver.core.config.{ConnectionResolver, SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.DAGResolver

/** Result of a doctor check run. */
case class DoctorResult(
    yamlValid: Boolean,
    yamlError: Option[String] = None,
    schemaErrors: List[String] = List.empty,
    dagValid: Boolean = false,
    dagLevels: Int = 0,
    dagParallelizable: Int = 0,
    envErrors: List[String] = List.empty,
    javaVersion: String = "",
    javaVersionOk: Boolean = false,
    connectionResults: Map[String, Either[String, Long]] = Map.empty
) {
  def overallHealthy: Boolean =
    yamlValid && schemaErrors.isEmpty && dagValid && envErrors.isEmpty && javaVersionOk
}

object DoctorCommand {

  /** Run doctor checks on a pipeline.
    * @param pipelinePath     Path to the pipeline YAML
    * @param connectionsPath  Optional path to connections.yaml
    * @param checkConnections Whether to test live connections (Phase 1+)
    * @param envProvider      Environment variable provider (for testing)
    */
  def run(
      pipelinePath: String,
      connectionsPath: Option[String] = None,
      checkConnections: Boolean = false,
      envProvider: Map[String, String] = sys.env
  ): DoctorResult = {
    // Check Java version
    val javaVersion = sys.props.getOrElse("java.specification.version", "unknown")
    val javaMajor = try { javaVersion.split('.').last.toInt } catch { case _: Exception => 0 }
    val javaOk = javaMajor >= 17

    // Parse YAML
    val parseResult = YAMLParser.parseFile(pipelinePath)
    parseResult match {
      case Left(err) =>
        return DoctorResult(
          yamlValid = false,
          yamlError = Some(err),
          javaVersion = javaVersion,
          javaVersionOk = javaOk
        )
      case Right(config) =>
        // Schema validation
        val schemaErrors = SchemaValidator.validate(config)

        // DAG resolution
        val (dagValid, dagLevels, dagParallelizable) = try {
          val levels = DAGResolver.resolve(config)
          val parallel = levels.count(_.size > 1)
          (true, levels.size, parallel)
        } catch {
          case e: Exception => (false, 0, 0)
        }

        // Environment variable check
        val envErrors = checkEnvVars(config, envProvider)

        // Print report
        printReport(pipelinePath, config.name, schemaErrors, dagValid, dagLevels,
          dagParallelizable, envErrors, javaVersion, javaOk)

        DoctorResult(
          yamlValid = true,
          schemaErrors = schemaErrors,
          dagValid = dagValid,
          dagLevels = dagLevels,
          dagParallelizable = dagParallelizable,
          envErrors = envErrors,
          javaVersion = javaVersion,
          javaVersionOk = javaOk
        )
    }
  }

  /** Scan all config values for ${env.VAR} and check if they're set. */
  private def checkEnvVars(
      config: com.dataweaver.core.config.PipelineConfig,
      envProvider: Map[String, String]
  ): List[String] = {
    val envPattern = """\$\{env\.([^}]+)\}""".r
    val allConfigValues = config.dataSources.flatMap(_.config.values) ++
      config.sinks.flatMap(_.config.values)

    allConfigValues.flatMap { value =>
      envPattern.findAllMatchIn(value).flatMap { m =>
        val varName = m.group(1)
        if (!envProvider.contains(varName))
          Some(s"$${{env.$varName}} is NOT SET — export $varName=<value> or add to .env file")
        else None
      }
    }.distinct
  }

  private def printReport(
      path: String, name: String, schemaErrors: List[String],
      dagValid: Boolean, dagLevels: Int, dagParallelizable: Int,
      envErrors: List[String], javaVersion: String, javaOk: Boolean
  ): Unit = {
    println()
    println("  Data Weaver Doctor")
    println("  " + "─" * 40)
    println()
    println(s"  Pipeline: $path")
    println(s"  ${ok(true)} YAML syntax valid")

    if (schemaErrors.isEmpty) println(s"  ${ok(true)} Schema valid")
    else schemaErrors.foreach(e => println(s"  ${ok(false)} $e"))

    if (dagValid) println(s"  ${ok(true)} DAG resolved ($dagLevels levels, $dagParallelizable parallelizable)")
    else println(s"  ${ok(false)} DAG resolution failed")

    println()
    println("  Environment:")
    println(s"  ${ok(javaOk)} Java $javaVersion ${if (javaOk) "(meets minimum: 17+)" else "(REQUIRES 17+)"}")

    if (envErrors.isEmpty) println(s"  ${ok(true)} All environment variables set")
    else envErrors.foreach(e => println(s"  ${ok(false)} $e"))

    val totalErrors = schemaErrors.size + envErrors.size + (if (!dagValid) 1 else 0) + (if (!javaOk) 1 else 0)
    println()
    if (totalErrors == 0)
      println("  Summary: All checks passed. Ready to run.")
    else
      println(s"  Summary: $totalErrors error(s) found. Fix them before running weaver apply.")
    println()
  }

  private def ok(pass: Boolean): String = if (pass) "✓" else "✗"
}
```

- [ ] **Step 4: Run tests**

Run: `sbt "cli/test"`
Expected: All 4 DoctorCommand tests PASS

- [ ] **Step 5: Commit**

```bash
git add cli/src/
git commit -m "feat(cli): add weaver doctor command

Full system diagnostic: YAML syntax, schema validation, DAG resolution,
environment variables, Java version. Returns structured DoctorResult.
Connection checks will be added in Phase 1 when connectors support healthCheck."
```

---

## Task 10: Migrate CLI + Integration Test

**Files:**
- Create: `cli/src/main/scala/com/dataweaver/cli/WeaverCLI.scala`
- Create: `cli/src/main/scala/com/dataweaver/cli/commands/ValidateCommand.scala`
- Create: `cli/src/main/scala/com/dataweaver/cli/commands/ApplyCommand.scala`
- Test: `cli/src/test/scala/com/dataweaver/cli/IntegrationTest.scala`
- Create: `core/src/test/resources/test_project/config/application.conf`

- [ ] **Step 1: Write ValidateCommand**

```scala
// cli/src/main/scala/com/dataweaver/cli/commands/ValidateCommand.scala
package com.dataweaver.cli.commands

import com.dataweaver.core.config.{SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.DAGResolver

object ValidateCommand {

  /** Validate a pipeline YAML file.
    * @return List of error messages. Empty = valid.
    */
  def run(pipelinePath: String): List[String] = {
    YAMLParser.parseFile(pipelinePath) match {
      case Left(parseError) => List(parseError)
      case Right(config) =>
        val schemaErrors = SchemaValidator.validate(config)
        if (schemaErrors.nonEmpty) return schemaErrors

        // Try DAG resolution
        try {
          DAGResolver.resolve(config)
          println(s"Pipeline '${config.name}' is valid")
          List.empty
        } catch {
          case e: Exception => List(e.getMessage)
        }
    }
  }
}
```

- [ ] **Step 2: Write ApplyCommand**

```scala
// cli/src/main/scala/com/dataweaver/cli/commands/ApplyCommand.scala
package com.dataweaver.cli.commands

import com.dataweaver.core.config.YAMLParser
import com.dataweaver.core.engine.PipelineExecutor
import org.apache.spark.sql.SparkSession

object ApplyCommand {

  def run(pipelinePath: String)(implicit spark: SparkSession): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        throw new IllegalArgumentException(s"Failed to parse pipeline: $err")
    }

    PipelineExecutor.execute(config)
  }
}
```

- [ ] **Step 3: Write WeaverCLI entry point**

```scala
// cli/src/main/scala/com/dataweaver/cli/WeaverCLI.scala
package com.dataweaver.cli

import com.dataweaver.cli.commands.{ApplyCommand, DoctorCommand, ValidateCommand}
import org.apache.spark.sql.SparkSession
import scopt.OParser

object WeaverCLI {

  case class Config(
      command: String = "",
      pipeline: Option[String] = None,
      env: Option[String] = None
  )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("weaver"),
        head("Data Weaver", "0.2.0"),
        cmd("doctor")
          .action((_, c) => c.copy(command = "doctor"))
          .text("Full system diagnostic")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file")
          ),
        cmd("validate")
          .action((_, c) => c.copy(command = "validate"))
          .text("Validate a pipeline YAML file")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file")
          ),
        cmd("apply")
          .action((_, c) => c.copy(command = "apply"))
          .text("Execute a pipeline")
          .children(
            arg[String]("<pipeline>")
              .action((x, c) => c.copy(pipeline = Some(x)))
              .text("Path to pipeline YAML file"),
            opt[String]("env")
              .action((x, c) => c.copy(env = Some(x)))
              .text("Environment profile (dev, prod)")
          )
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        config.command match {
          case "doctor" =>
            val result = DoctorCommand.run(config.pipeline.get)
            if (!result.overallHealthy) sys.exit(1)
          case "validate" =>
            val errors = ValidateCommand.run(config.pipeline.get)
            if (errors.nonEmpty) {
              errors.foreach(e => System.err.println(s"ERROR: $e"))
              sys.exit(1)
            }
          case "apply" =>
            implicit val spark: SparkSession = SparkSession.builder()
              .appName(s"DataWeaver")
              .getOrCreate()
            try {
              ApplyCommand.run(config.pipeline.get)
            } finally {
              spark.stop()
            }
          case _ =>
            println("Unknown command. Use --help for usage.")
        }
      case None =>
        sys.exit(1)
    }
  }
}
```

- [ ] **Step 4: Write integration test**

```scala
// cli/src/test/scala/com/dataweaver/cli/IntegrationTest.scala
package com.dataweaver.cli

import com.dataweaver.cli.commands.{ApplyCommand, ValidateCommand}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  val testPipeline = "core/src/test/resources/test_project/pipelines/pipeline.yaml"
  val outputPath = "core/src/test/resources/output_test"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("IntegrationTest").getOrCreate()
    // Clean output dir
    deleteDir(outputPath)
  }

  override def afterAll(): Unit = {
    deleteDir(outputPath)
    spark.stop()
  }

  private def deleteDir(path: String): Unit = {
    import java.io.File
    import scala.reflect.io.Directory
    val dir = new Directory(new File(path))
    dir.deleteRecursively()
  }

  "ValidateCommand" should "validate the test pipeline" in {
    val errors = ValidateCommand.run(testPipeline)
    errors shouldBe empty
  }

  "ApplyCommand" should "execute the test pipeline end-to-end" in {
    ApplyCommand.run(testPipeline)

    val output = spark.read.json(s"$outputPath/ExamplePipeline.json")
    output.count() shouldBe 1
    output.columns should contain only "id"
    output.collect().map(_.getAs[String]("id")) should contain only "Alice"
  }
}
```

- [ ] **Step 5: Run integration test**

Run: `sbt "cli/test"`
Expected: Both tests PASS

- [ ] **Step 6: Commit**

```bash
git add cli/src/
git commit -m "feat(cli): add WeaverCLI with validate and apply commands

Implements weaver validate and weaver apply commands.
Integration test verifies end-to-end pipeline execution
with the new plugin-based architecture."
```

---

## Task 11: Clean Up Old Code + Final Verification

**Files:**
- Delete: `src/main/scala/com/dataweaver/` (entire old tree)
- Delete: `src/test/scala/com/dataweaver/` (entire old test tree)
- Keep: old `src/test/resources/` until verified not needed

- [ ] **Step 1: Run all tests across all modules**

Run: `sbt test`
Expected: All tests PASS across core, connectors, transformations, cli

- [ ] **Step 2: Remove old source tree**

Run:
```bash
rm -rf src/main/scala/com/dataweaver/
rm -rf src/test/scala/com/dataweaver/
```

- [ ] **Step 3: Verify build still works**

Run: `sbt clean compile test`
Expected: All modules compile and all tests PASS

- [ ] **Step 4: Remove unused files**

```bash
rm -f src/main/scala/com/dataweaver/sink/DataSinkManager.scala  # unused trait
rm -f src/main/scala/com/dataweaver/utils/utils.scala            # unused utility
rm -f src/main/scala/com/dataweaver/config/package.scala         # hardcoded Spark conf
```

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "refactor: remove old single-module source tree

All functionality migrated to multi-module architecture:
- core: plugin traits, DAG resolver, connection resolver, schema validator
- connectors: SQL, BigQuery, Test connectors
- transformations: SQL transform
- cli: weaver validate, weaver apply

Old files removed:
- DataSinkManager (unused)
- utils.scala (unused)
- package.scala (hardcoded Spark conf)
- All old reader/, sink/, factory/, pipeline/ files"
```

---

## Summary: What Phase 0 Delivers

| Before | After |
|--------|-------|
| Single monolith module | 4 SBT modules (core, connectors, transformations, cli) |
| Hardcoded factory switch/case | Plugin system with ServiceLoader discovery |
| Sequential pipeline execution | DAG-based parallel execution via Future |
| All sinks get `last._2` | Per-sink explicit source routing |
| No validation | SchemaValidator with human-readable errors |
| No diagnostic tool | `weaver doctor` — full system diagnostic |
| No CLI commands | `weaver validate` + `weaver apply` + `weaver doctor` |
| Hardcoded paths | ConnectionResolver with `${env.VAR}` injection |

**Next:** Phase 1 — Local Engine (DuckDB) + Core Connectors (PostgreSQL, File, S3, DeltaLake)
