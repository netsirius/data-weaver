# Data Weaver Connector SDK

Build custom connectors for Data Weaver. A connector is a Scala class that implements `SourceConnector` or `SinkConnector` and is packaged as a JAR.

## Quick Start

### 1. Create a new SBT project

```scala
// build.sbt
libraryDependencies += "com.dataweaver" %% "data-weaver-core" % "0.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.2" % "provided"
```

### 2. Implement the trait

**Source connector:**
```scala
package com.example.connector

import com.dataweaver.core.plugin.SourceConnector
import org.apache.spark.sql.{DataFrame, SparkSession}

class MySourceConnector extends SourceConnector {
  def connectorType: String = "MySource"

  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    val host = config.getOrElse("host", throw new IllegalArgumentException("host required"))
    // Read data and return as DataFrame
    spark.read.format("jdbc").option("url", s"jdbc:mydb://$host").load()
  }

  override def healthCheck(config: Map[String, String]): Either[String, Long] = {
    // Optional: test connectivity for `weaver doctor`
    try {
      val start = System.currentTimeMillis()
      // ... test connection ...
      Right(System.currentTimeMillis() - start)
    } catch {
      case e: Exception => Left(s"Connection failed: ${e.getMessage}")
    }
  }
}
```

**Sink connector:**
```scala
class MySinkConnector extends SinkConnector {
  def connectorType: String = "MySink"

  def write(data: DataFrame, pipelineName: String, config: Map[String, String])(implicit
      spark: SparkSession
  ): Unit = {
    val target = config.getOrElse("target", throw new IllegalArgumentException("target required"))
    data.write.format("myformat").save(target)
  }
}
```

### 3. Register via ServiceLoader

Data Weaver discovers connectors automatically using Java's ServiceLoader mechanism. You need to create a plain text file **inside your connector project** that tells ServiceLoader which classes implement the connector interface.

Create this file in your project at `src/main/resources/META-INF/services/com.dataweaver.core.plugin.SourceConnector`:

```
com.example.connector.MySourceConnector
```

Your project structure should look like this:
```
my-connector/
├── src/
│   └── main/
│       ├── scala/com/example/connector/
│       │   └── MySourceConnector.scala
│       └── resources/META-INF/services/
│           └── com.dataweaver.core.plugin.SourceConnector   ← text file
└── build.sbt
```

The file name IS the interface name. The file content is the full class name of your implementation (one per line if you have multiple). When you package your project as a JAR and place it in `~/.weaver/plugins/`, Data Weaver finds and loads your connector automatically at startup.

For sink connectors, create `com.dataweaver.core.plugin.SinkConnector` instead.
For transforms, create `com.dataweaver.core.plugin.TransformPlugin`.

### 4. Package and install

```bash
sbt assembly
cp target/scala-2.13/my-connector.jar ~/.weaver/plugins/
```

Or publish to Maven Central and install via:
```bash
weaver install com.example:my-connector:1.0.0
```

## Config Map

The `config` parameter contains all key-value pairs from the YAML `config:` section. The `id`, `query`, and `connection` fields from the dataSource are also injected automatically.

## Best Practices

1. **Validate early**: throw `IllegalArgumentException` for missing required config
2. **Implement healthCheck**: enables `weaver doctor` support
3. **Use Spark formats**: prefer `spark.read.format(...)` for compatibility with both Spark and DuckDB engines
4. **Handle credentials**: expect `${env.VAR}` resolved values (the resolver runs before your connector)
5. **Log with Log4j**: use `LogManager.getLogger(getClass)` for consistency
