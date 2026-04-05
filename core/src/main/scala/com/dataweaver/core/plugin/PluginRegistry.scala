package com.dataweaver.core.plugin

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

object PluginRegistry {
  private var sources: Map[String, SourceConnector] = Map.empty
  private var sinks: Map[String, SinkConnector] = Map.empty
  private var transforms: Map[String, TransformPlugin] = Map.empty
  private var initialized = false

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
