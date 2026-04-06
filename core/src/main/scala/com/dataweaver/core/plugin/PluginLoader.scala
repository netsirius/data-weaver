package com.dataweaver.core.plugin

import org.apache.log4j.LogManager

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Loads external plugin JARs from the plugins directory.
  * Scans ~/.weaver/plugins/ (and project-local plugins/) for JARs,
  * adds them to the classpath, and discovers connectors via ServiceLoader.
  */
object PluginLoader {
  private val logger = LogManager.getLogger(getClass)

  /** Default plugin directories, checked in order. */
  val pluginDirs: List[String] = List(
    sys.props.getOrElse("user.home", ".") + "/.weaver/plugins",
    "plugins"
  )

  /** Load all plugin JARs from plugin directories and register discovered connectors. */
  def loadExternalPlugins(): Unit = {
    val jarFiles = pluginDirs.flatMap(findJars).distinct

    if (jarFiles.isEmpty) {
      logger.info("No external plugin JARs found")
      return
    }

    logger.info(s"Found ${jarFiles.size} plugin JAR(s)")

    val urls = jarFiles.map(f => f.toURI.toURL).toArray
    val classLoader = new URLClassLoader(urls, getClass.getClassLoader)

    // Discover and register sources
    val sources = ServiceLoader.load(classOf[SourceConnector], classLoader)
    sources.asScala.foreach { connector =>
      logger.info(s"  Loaded source: ${connector.connectorType}")
      PluginRegistry.registerSource(connector)
    }

    // Discover and register sinks
    val sinks = ServiceLoader.load(classOf[SinkConnector], classLoader)
    sinks.asScala.foreach { connector =>
      logger.info(s"  Loaded sink: ${connector.connectorType}")
      PluginRegistry.registerSink(connector)
    }

    // Discover and register transforms
    val transforms = ServiceLoader.load(classOf[TransformPlugin], classLoader)
    transforms.asScala.foreach { plugin =>
      logger.info(s"  Loaded transform: ${plugin.transformType}")
      PluginRegistry.registerTransform(plugin)
    }
  }

  /** Find all .jar files in a directory. */
  private def findJars(dirPath: String): List[File] = {
    val dir = new File(dirPath)
    if (!dir.exists() || !dir.isDirectory) return List.empty
    dir.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(".jar"))
      .toList
  }

  /** Ensure plugin directories exist. */
  def ensurePluginDirs(): Unit = {
    pluginDirs.foreach { path =>
      val dir = new File(path)
      if (!dir.exists()) dir.mkdirs()
    }
  }
}
