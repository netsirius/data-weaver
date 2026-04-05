package com.dataweaver.cli.commands

import com.dataweaver.core.config.{SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.{DAGResolver, SourceNode, TransformNode}
import com.dataweaver.core.plugin.PluginRegistry

object PlanCommand {

  def run(pipelinePath: String): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    val errors = SchemaValidator.validate(config)
    if (errors.nonEmpty) {
      errors.foreach(e => System.err.println(s"ERROR: $e"))
      return
    }

    val levels = DAGResolver.resolve(config)

    println()
    println(s"  Pipeline: ${config.name}")
    println(s"  Engine: ${config.engine}")
    println(s"  " + "\u2500" * 50)
    println()

    println("  Data Sources:")
    config.dataSources.foreach { ds =>
      val available = PluginRegistry.getSource(ds.`type`).isDefined
      val status = if (available) "\u2713" else "\u2717 NOT AVAILABLE"
      println(s"    $status ${ds.id} (${ds.`type`})")
      ds.connection.foreach(c => println(s"      connection: $c"))
    }

    println()
    println("  Transformations:")
    config.transformations.foreach { t =>
      val available = PluginRegistry.getTransform(t.`type`).isDefined
      val status = if (available) "\u2713" else "\u2717 NOT AVAILABLE"
      println(s"    $status ${t.id} (${t.`type`}) <- [${t.sources.mkString(", ")}]")
    }

    println()
    println("  Sinks:")
    config.sinks.foreach { s =>
      val available = PluginRegistry.getSink(s.`type`).isDefined
      val status = if (available) "\u2713" else "\u2717 NOT AVAILABLE"
      println(s"    $status ${s.id} (${s.`type`}) <- ${s.source.getOrElse("(auto)")}")
    }

    println()
    println(s"  Execution Plan: ${levels.size} levels")
    levels.zipWithIndex.foreach { case (level, i) =>
      val parallel = if (level.size > 1) " [PARALLEL]" else ""
      println(s"    Level $i$parallel: ${level.map(_.id).mkString(", ")}")
    }
    println()
  }
}
