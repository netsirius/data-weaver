package com.dataweaver.cli.commands

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

/** Scaffolds connector projects, private registries, and transform plugins.
  *
  * Usage:
  *   weaver scaffold connector my-redis-connector
  *   weaver scaffold connector my-salesforce-connector --type sink
  *   weaver scaffold connector my-connector --type both
  *   weaver scaffold transform my-custom-transform
  *   weaver scaffold registry my-company-registry
  */
object ScaffoldCommand {

  def run(what: String, name: String, connectorKind: String = "source"): Unit = {
    what match {
      case "connector" => scaffoldConnector(name, connectorKind)
      case "transform" => scaffoldTransform(name)
      case "registry"  => scaffoldRegistry(name)
      case other       =>
        System.err.println(s"Unknown scaffold type: '$other'")
        System.err.println("Usage: weaver scaffold connector|transform|registry <name>")
    }
  }

  /** Scaffold a complete connector project ready to build and install. */
  private def scaffoldConnector(name: String, kind: String): Unit = {
    val projectDir = Paths.get(name)
    if (Files.exists(projectDir)) {
      System.err.println(s"Directory '$name' already exists.")
      return
    }

    // Derive class names from project name
    val className = name.split("[-_]").map(_.capitalize).mkString("")
    val packageName = s"com.dataweaver.connectors.${name.replace("-", "").replace("_", "")}"
    val packagePath = packageName.replace('.', '/')

    // Create directory structure
    val srcDir = projectDir.resolve(s"src/main/scala/$packagePath")
    val resourceDir = projectDir.resolve("src/main/resources/META-INF/services")
    val testDir = projectDir.resolve(s"src/test/scala/$packagePath")
    Files.createDirectories(srcDir)
    Files.createDirectories(resourceDir)
    Files.createDirectories(testDir)

    // build.sbt
    writeFile(projectDir.resolve("build.sbt").toFile,
      s"""name := "$name"
         |version := "0.1.0"
         |scalaVersion := "2.13.14"
         |
         |libraryDependencies ++= Seq(
         |  "com.dataweaver"   %% "data-weaver-core" % "0.2.0" % "provided",
         |  "org.apache.spark" %% "spark-sql"        % "4.0.2" % "provided",
         |  "org.scalatest"    %% "scalatest"         % "3.2.19" % Test
         |)
         |
         |// Package as fat JAR for installation
         |assembly / assemblyJarName := s"$${name.value}-$${version.value}.jar"
         |""".stripMargin)

    // Source connector
    if (kind == "source" || kind == "both") {
      writeFile(srcDir.resolve(s"${className}SourceConnector.scala").toFile,
        s"""package $packageName
           |
           |import com.dataweaver.core.plugin.SourceConnector
           |import org.apache.spark.sql.{DataFrame, SparkSession}
           |
           |/** TODO: Implement your source connector.
           |  *
           |  * Pipeline YAML usage:
           |  * {{{
           |  * dataSources:
           |  *   - id: my_source
           |  *     type: $className
           |  *     connection: my-connection
           |  *     config:
           |  *       option1: value1
           |  * }}}
           |  */
           |class ${className}SourceConnector extends SourceConnector {
           |
           |  def connectorType: String = "$className"
           |
           |  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
           |    // TODO: Implement data reading logic
           |    // config contains all key-value pairs from the YAML config section
           |    // plus "id" and "query" from the dataSource definition
           |    //
           |    // Example:
           |    //   val host = config.getOrElse("host",
           |    //     throw new IllegalArgumentException("$className: 'host' is required"))
           |    //   spark.read.format("jdbc").option("url", s"jdbc:mydb://$$host").load()
           |
           |    throw new UnsupportedOperationException("TODO: implement read()")
           |  }
           |
           |  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
           |    // TODO: Implement connectivity test for 'weaver doctor'
           |    // Return Right(latencyMs) on success, Left(errorMessage) on failure
           |    //
           |    // Example:
           |    //   val start = System.currentTimeMillis()
           |    //   // ... test connection ...
           |    //   Right(System.currentTimeMillis() - start)
           |
           |    Left("Health check not yet implemented")
           |  }
           |}
           |""".stripMargin)

      writeFile(resourceDir.resolve("com.dataweaver.core.plugin.SourceConnector").toFile,
        s"$packageName.${className}SourceConnector\n")

      writeFile(testDir.resolve(s"${className}SourceConnectorTest.scala").toFile,
        s"""package $packageName
           |
           |import org.scalatest.flatspec.AnyFlatSpec
           |import org.scalatest.matchers.should.Matchers
           |
           |class ${className}SourceConnectorTest extends AnyFlatSpec with Matchers {
           |
           |  "${className}SourceConnector" should "have correct connector type" in {
           |    val connector = new ${className}SourceConnector()
           |    connector.connectorType shouldBe "$className"
           |  }
           |
           |  // TODO: Add integration tests with a real or mocked data source
           |}
           |""".stripMargin)
    }

    // Sink connector
    if (kind == "sink" || kind == "both") {
      writeFile(srcDir.resolve(s"${className}SinkConnector.scala").toFile,
        s"""package $packageName
           |
           |import com.dataweaver.core.plugin.SinkConnector
           |import org.apache.spark.sql.{DataFrame, SparkSession}
           |
           |/** TODO: Implement your sink connector.
           |  *
           |  * Pipeline YAML usage:
           |  * {{{
           |  * sinks:
           |  *   - id: my_sink
           |  *     type: $className
           |  *     source: transform_id
           |  *     config:
           |  *       option1: value1
           |  * }}}
           |  */
           |class ${className}SinkConnector extends SinkConnector {
           |
           |  def connectorType: String = "$className"
           |
           |  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
           |      spark: SparkSession
           |  ): Unit = {
           |    // TODO: Implement data writing logic
           |    // config contains all key-value pairs from the YAML config section
           |    //
           |    // Example:
           |    //   val path = config.getOrElse("path",
           |    //     throw new IllegalArgumentException("$className: 'path' is required"))
           |    //   data.write.format("myformat").save(path)
           |
           |    throw new UnsupportedOperationException("TODO: implement write()")
           |  }
           |}
           |""".stripMargin)

      writeFile(resourceDir.resolve("com.dataweaver.core.plugin.SinkConnector").toFile,
        s"$packageName.${className}SinkConnector\n")
    }

    // README
    writeFile(projectDir.resolve("README.md").toFile,
      s"""# $name
         |
         |Data Weaver connector for $className.
         |
         |## Build
         |
         |```bash
         |sbt assembly
         |```
         |
         |## Install
         |
         |```bash
         |# Option 1: weaver install
         |weaver install target/scala-2.13/$name-0.1.0.jar
         |
         |# Option 2: manual copy
         |cp target/scala-2.13/$name-0.1.0.jar ~/.weaver/plugins/
         |```
         |
         |## Usage
         |
         |```yaml
         |dataSources:
         |  - id: my_data
         |    type: $className
         |    config:
         |      option1: value1
         |```
         |
         |## Development
         |
         |1. Implement the connector in `src/main/scala/`
         |2. Run tests: `sbt test`
         |3. Build JAR: `sbt assembly`
         |4. Install: `weaver install target/scala-2.13/$name-0.1.0.jar`
         |5. Verify: `weaver list connectors` should show "$className"
         |""".stripMargin)

    // .gitignore
    writeFile(projectDir.resolve(".gitignore").toFile,
      """**/target/
        |.idea/
        |.bsp/
        |*.class
        |.DS_Store
        |""".stripMargin)

    println()
    println(s"  Connector project '$name' created!")
    println()
    println("  Structure:")
    println(s"    $name/")
    println(s"    ├── build.sbt")
    if (kind == "source" || kind == "both")
      println(s"    ├── src/main/scala/.../  ${className}SourceConnector.scala")
    if (kind == "sink" || kind == "both")
      println(s"    ├── src/main/scala/.../  ${className}SinkConnector.scala")
    println(s"    ├── src/main/resources/   META-INF/services (auto-registered)")
    println(s"    ├── src/test/scala/.../   Tests")
    println(s"    └── README.md")
    println()
    println("  Next steps:")
    println(s"    cd $name")
    println(s"    # Edit the TODO sections in the connector code")
    println(s"    sbt test")
    println(s"    sbt assembly")
    println(s"    weaver install target/scala-2.13/$name-0.1.0.jar")
    println()
  }

  /** Scaffold a custom transform plugin project. */
  private def scaffoldTransform(name: String): Unit = {
    val projectDir = Paths.get(name)
    if (Files.exists(projectDir)) {
      System.err.println(s"Directory '$name' already exists.")
      return
    }

    val className = name.split("[-_]").map(_.capitalize).mkString("")
    val packageName = s"com.dataweaver.transforms.${name.replace("-", "").replace("_", "")}"
    val packagePath = packageName.replace('.', '/')

    val srcDir = projectDir.resolve(s"src/main/scala/$packagePath")
    val resourceDir = projectDir.resolve("src/main/resources/META-INF/services")
    Files.createDirectories(srcDir)
    Files.createDirectories(resourceDir)

    writeFile(projectDir.resolve("build.sbt").toFile,
      s"""name := "$name"
         |version := "0.1.0"
         |scalaVersion := "2.13.14"
         |
         |libraryDependencies ++= Seq(
         |  "com.dataweaver"   %% "data-weaver-core" % "0.2.0" % "provided",
         |  "org.apache.spark" %% "spark-sql"        % "4.0.2" % "provided",
         |  "org.scalatest"    %% "scalatest"         % "3.2.19" % Test
         |)
         |""".stripMargin)

    writeFile(srcDir.resolve(s"${className}Plugin.scala").toFile,
      s"""package $packageName
         |
         |import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
         |import org.apache.spark.sql.{DataFrame, SparkSession}
         |
         |class ${className}Plugin extends TransformPlugin {
         |  def transformType: String = "$className"
         |
         |  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
         |      spark: SparkSession
         |  ): DataFrame = {
         |    val df = inputs.values.head
         |    // TODO: implement transformation
         |    df
         |  }
         |}
         |""".stripMargin)

    writeFile(resourceDir.resolve("com.dataweaver.core.plugin.TransformPlugin").toFile,
      s"$packageName.${className}Plugin\n")

    println(s"  Transform project '$name' created!")
    println(s"  Next: cd $name && edit src/.../${className}Plugin.scala")
  }

  /** Scaffold a private connector registry for an organization. */
  private def scaffoldRegistry(name: String): Unit = {
    val projectDir = Paths.get(name)
    if (Files.exists(projectDir)) {
      System.err.println(s"Directory '$name' already exists.")
      return
    }

    Files.createDirectories(projectDir.resolve("connectors"))

    writeFile(projectDir.resolve("README.md").toFile,
      s"""# $name — Private Connector Registry
         |
         |Private Data Weaver connector registry for your organization.
         |
         |## Structure
         |
         |```
         |$name/
         |├── connectors/              # Connector JARs
         |│   ├── connector-a-1.0.0.jar
         |│   └── connector-b-2.1.0.jar
         |├── registry.yaml            # Connector catalog
         |└── install.sh               # Team installer script
         |```
         |
         |## Publishing a connector
         |
         |1. Build your connector: `cd my-connector && sbt assembly`
         |2. Copy JAR to `connectors/`
         |3. Update `registry.yaml`
         |4. Push to your Git remote
         |
         |## Installing for your team
         |
         |```bash
         |# Clone the registry
         |git clone git@github.com:your-org/$name.git
         |
         |# Install all connectors
         |./$name/install.sh
         |
         |# Or install individual connectors
         |weaver install $name/connectors/connector-a-1.0.0.jar
         |```
         |""".stripMargin)

    writeFile(projectDir.resolve("registry.yaml").toFile,
      s"""# $name — Connector Registry
         |# List your organization's private connectors here
         |
         |connectors:
         |  # - name: my-connector
         |  #   version: 1.0.0
         |  #   jar: connectors/my-connector-1.0.0.jar
         |  #   description: Custom connector for internal system
         |  #   type: source  # source | sink | both
         |""".stripMargin)

    writeFile(projectDir.resolve("install.sh").toFile,
      s"""#!/bin/bash
         |# Install all connectors from this registry
         |set -e
         |SCRIPT_DIR="$$( cd "$$( dirname "$${BASH_SOURCE[0]}" )" && pwd )"
         |
         |echo "Installing connectors from $name..."
         |for jar in "$$SCRIPT_DIR"/connectors/*.jar; do
         |  if [ -f "$$jar" ]; then
         |    echo "  Installing: $$(basename $$jar)"
         |    weaver install "$$jar"
         |  fi
         |done
         |echo "Done!"
         |""".stripMargin)

    new File(projectDir.resolve("install.sh").toString).setExecutable(true)

    println()
    println(s"  Private registry '$name' created!")
    println()
    println("  Structure:")
    println(s"    $name/")
    println(s"    ├── connectors/      # Place connector JARs here")
    println(s"    ├── registry.yaml    # Connector catalog")
    println(s"    ├── install.sh       # Team installer script")
    println(s"    └── README.md")
    println()
    println("  Share with your team via Git:")
    println(s"    cd $name && git init && git add -A && git commit -m 'init registry'")
    println()
  }

  private def writeFile(file: File, content: String): Unit = {
    val writer = new PrintWriter(file)
    try writer.write(content)
    finally writer.close()
  }
}
