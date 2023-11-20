ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "data-weaver",
    version := "0.1.0-SNAPSHOT"
  )

scalaVersion := "2.13.12"

// Spark version compatible con tu versión de Scala
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  // Apache Spark
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // Cli
  "com.github.scopt" %% "scopt" % "4.1.0",

  //logging
  "org.slf4j" % "slf4j-api" % "1.7.36",

  //YAML
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-yaml" % "0.15.1",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",

  // Api
  "com.typesafe.play" %% "play" % "2.9.0", // Versión más reciente al momento
  "com.typesafe.play" %% "play-slick" % "5.2.0", // Versión más reciente al momento
  "com.typesafe.play" %% "play-json" % "2.10.3", // Versión más reciente al momento

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test, // Versión más reciente al momento
  "org.scalactic" %% "scalactic" % "3.2.17" % Test // Versión más reciente al momento
)

// Añadir repositorios adicionales si es necesario
resolvers ++= Seq(
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/"
)

// Merge strategy rules
assembly / assemblyMergeStrategy := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*)
    if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", _@_*) =>
    MergeStrategy.discard
  case _ =>
    MergeStrategy.first
}

// Especificar la clase principal
Compile / mainClass := Some("com.dataweaver.cli.CommandLineInterface")
assembly / assemblyJarName := s"${name.value}.jar"
assembly / test := {}