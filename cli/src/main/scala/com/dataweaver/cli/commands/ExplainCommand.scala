package com.dataweaver.cli.commands

import com.dataweaver.core.config.YAMLParser
import com.dataweaver.core.dag.{DAGResolver, SourceNode, TransformNode}

object ExplainCommand {

  def run(pipelinePath: String): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    val levels = DAGResolver.resolve(config)

    println()
    println(s"  DAG for pipeline: ${config.name}")
    println(s"  " + "\u2500" * 50)
    println()

    levels.zipWithIndex.foreach { case (level, i) =>
      level.foreach { node =>
        val nodeType = node match {
          case _: SourceNode    => "SOURCE"
          case _: TransformNode => "TRANSFORM"
        }
        val deps = node.dependencies
        val arrow = if (deps.nonEmpty) s" <- [${deps.mkString(", ")}]" else ""
        println(s"  Level $i | [$nodeType] ${node.id}$arrow")
      }
      if (i < levels.size - 1) println(s"         |")
    }

    println()
    println("  Sink Routing:")
    config.sinks.foreach { s =>
      println(s"    ${s.source.getOrElse("?")} --> [SINK] ${s.id} (${s.`type`})")
    }

    val parallelLevels = levels.count(_.size > 1)
    val totalNodes = levels.map(_.size).sum
    println()
    println(s"  Summary: $totalNodes nodes, ${levels.size} levels, $parallelLevels parallelizable")
    println()
  }
}
