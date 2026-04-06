package com.dataweaver.cli.commands

import com.dataweaver.core.plugin.PluginLoader
import org.apache.log4j.LogManager

import java.io.{File, FileOutputStream}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/** Installs connector plugins from Maven Central or a custom registry.
  *
  * Usage:
  *   weaver install connector-kafka                          # shorthand
  *   weaver install com.dataweaver:connector-kafka:1.0.0     # full Maven coords
  *   weaver install /path/to/connector.jar                   # local JAR
  */
object InstallCommand {
  private val logger = LogManager.getLogger(getClass)
  private val httpClient = HttpClient.newHttpClient()

  private val pluginDir = sys.props.getOrElse("user.home", ".") + "/.weaver/plugins"

  /** Known shorthand mappings for official connectors. */
  private val shorthands = Map(
    "connector-kafka"         -> ("com.dataweaver", "data-weaver-connector-kafka", "0.2.0"),
    "connector-mongodb"       -> ("com.dataweaver", "data-weaver-connector-mongodb", "0.2.0"),
    "connector-elasticsearch" -> ("com.dataweaver", "data-weaver-connector-elasticsearch", "0.2.0"),
    "connector-redis"         -> ("com.dataweaver", "data-weaver-connector-redis", "0.2.0"),
    "connector-s3"            -> ("com.dataweaver", "data-weaver-connector-s3", "0.2.0"),
    "connector-neo4j"         -> ("com.dataweaver", "data-weaver-connector-neo4j", "0.2.0")
  )

  def run(artifact: String): Unit = {
    PluginLoader.ensurePluginDirs()

    // Local JAR file
    if (artifact.endsWith(".jar") && new File(artifact).exists()) {
      installLocalJar(artifact)
      return
    }

    // Shorthand or Maven coordinates
    val (groupId, artifactId, version) = shorthands.getOrElse(artifact, parseMavenCoords(artifact))

    println(s"Installing $groupId:$artifactId:$version...")

    val jarUrl = mavenCentralUrl(groupId, artifactId, version)
    val targetFile = new File(s"$pluginDir/$artifactId-$version.jar")

    if (targetFile.exists()) {
      println(s"  Already installed: ${targetFile.getName}")
      return
    }

    downloadJar(jarUrl, targetFile) match {
      case Right(_) =>
        println(s"  Downloaded to: ${targetFile.getAbsolutePath}")
        println(s"  Connector will be available on next weaver command.")
        println()
      case Left(error) =>
        System.err.println(s"  Failed to download: $error")
        System.err.println()
        System.err.println("  If this is a local JAR, use:")
        System.err.println(s"    weaver install /path/to/$artifactId.jar")
        System.err.println()
        System.err.println("  Or copy manually:")
        System.err.println(s"    cp /path/to/$artifactId.jar $pluginDir/")
    }
  }

  /** List installed plugins. */
  def listInstalled(): List[String] = {
    val dir = new File(pluginDir)
    if (!dir.exists()) return List.empty
    dir.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".jar"))
      .map(_.getName)
      .toList
  }

  private def installLocalJar(path: String): Unit = {
    val source = new File(path)
    val target = new File(s"$pluginDir/${source.getName}")

    java.nio.file.Files.copy(source.toPath, target.toPath,
      java.nio.file.StandardCopyOption.REPLACE_EXISTING)

    println(s"  Installed: ${target.getName}")
    println(s"  Location: ${target.getAbsolutePath}")
  }

  private def parseMavenCoords(coords: String): (String, String, String) = {
    coords.split(":") match {
      case Array(g, a, v) => (g, a, v)
      case Array(g, a)    => (g, a, "LATEST")
      case _              =>
        throw new IllegalArgumentException(
          s"Invalid artifact format: '$coords'. Expected: groupId:artifactId:version")
    }
  }

  private def mavenCentralUrl(groupId: String, artifactId: String, version: String): String = {
    val groupPath = groupId.replace('.', '/')
    s"https://repo1.maven.org/maven2/$groupPath/$artifactId/$version/$artifactId-$version.jar"
  }

  private def downloadJar(url: String, target: File): Either[String, Unit] = {
    try {
      val request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .GET()
        .build()

      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())

      if (response.statusCode() != 200) {
        return Left(s"HTTP ${response.statusCode()} from $url")
      }

      val fos = new FileOutputStream(target)
      try {
        response.body().transferTo(fos)
        Right(())
      } finally {
        fos.close()
      }
    } catch {
      case e: Exception => Left(e.getMessage)
    }
  }
}
