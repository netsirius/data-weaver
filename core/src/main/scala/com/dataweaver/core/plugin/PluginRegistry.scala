package com.dataweaver.core.plugin

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.Try

object PluginRegistry {
  private var sources: Map[String, SourceConnector] = Map.empty
  private var sinks: Map[String, SinkConnector] = Map.empty
  private var transforms: Map[String, TransformPlugin] = Map.empty
  private var initialized = false

  /** Initialize by discovering plugins via ServiceLoader.
    * Gracefully skips connectors whose dependencies are not on the classpath.
    */
  def init(): Unit = synchronized {
    if (!initialized) {
      safeLoad(classOf[SourceConnector]).foreach(registerSource)
      safeLoad(classOf[SinkConnector]).foreach(registerSink)
      safeLoad(classOf[TransformPlugin]).foreach(registerTransform)
      initialized = true
    }
  }

  /** Load services, skipping any that fail to instantiate (missing deps). */
  private def safeLoad[T](clazz: Class[T]): List[T] = {
    val loader = ServiceLoader.load(clazz)
    val results = scala.collection.mutable.ListBuffer[T]()
    val iterator = loader.iterator()
    while (Try(iterator.hasNext).getOrElse(false)) {
      Try(iterator.next()).foreach(results += _)
    }
    results.toList
  }

  def registerSource(connector: SourceConnector): Unit =
    sources += (connector.connectorType -> connector)
  def registerSink(connector: SinkConnector): Unit =
    sinks += (connector.connectorType -> connector)
  def registerTransform(plugin: TransformPlugin): Unit =
    transforms += (plugin.transformType -> plugin)

  def getSource(connectorType: String): Option[SourceConnector] = { init(); sources.get(connectorType) }
  def getSink(connectorType: String): Option[SinkConnector] = { init(); sinks.get(connectorType) }
  def getTransform(transformType: String): Option[TransformPlugin] = { init(); transforms.get(transformType) }

  def availableSources: Set[String] = { init(); sources.keySet }
  def availableSinks: Set[String] = { init(); sinks.keySet }
  def availableTransforms: Set[String] = { init(); transforms.keySet }

  private[core] def reset(): Unit = synchronized {
    sources = Map.empty; sinks = Map.empty; transforms = Map.empty; initialized = false
  }
}
