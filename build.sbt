ThisBuild / version := "0.2.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.dataweaver"

val sparkVersion = "4.0.2"

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
      "io.circe"         %% "circe-generic" % "0.14.10",
      "org.duckdb"        % "duckdb_jdbc"   % "1.1.3"
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
      "org.json4s"       %% "json4s-jackson" % "4.0.7",
      "org.postgresql"    % "postgresql" % "42.7.4",
      "org.apache.kafka"  % "kafka-clients" % "3.7.0" % "provided"
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
