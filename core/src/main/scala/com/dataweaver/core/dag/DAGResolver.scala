package com.dataweaver.core.dag

import com.dataweaver.core.config.{PipelineConfig, TransformationConfig, DataSourceConfig}

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

object DAGResolver {

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
        dependents(id).foreach { dep => inDegree(dep) -= 1 }
      }
    }

    levels
  }
}
