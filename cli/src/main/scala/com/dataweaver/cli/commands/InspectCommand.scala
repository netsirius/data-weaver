package com.dataweaver.cli.commands

import com.dataweaver.core.config.YAMLParser

object InspectCommand {

  def run(pipelinePath: String, transformId: String): Unit = {
    val config = YAMLParser.parseFile(pipelinePath) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"ERROR: $err")
        return
    }

    val allIds = config.dataSources.map(_.id) ++ config.transformations.map(_.id)
    if (!allIds.contains(transformId)) {
      System.err.println(s"ERROR: '$transformId' not found. Available: ${allIds.mkString(", ")}")
      return
    }

    config.dataSources.find(_.id == transformId).foreach { ds =>
      println()
      println(s"  Source: ${ds.id}")
      println(s"  Type: ${ds.`type`}")
      ds.connection.foreach(c => println(s"  Connection: $c"))
      if (ds.query.nonEmpty) println(s"  Query: ${ds.query.trim}")
      if (ds.config.nonEmpty)
        println(s"  Config: ${ds.config.map { case (k, v) => s"$k=$v" }.mkString(", ")}")
      println()
    }

    config.transformations.find(_.id == transformId).foreach { t =>
      println()
      println(s"  Transform: ${t.id}")
      println(s"  Type: ${t.`type`}")
      println(s"  Sources: [${t.sources.mkString(", ")}]")
      t.query.foreach(q => println(s"  Query: ${q.trim}"))
      t.action.foreach(a => println(s"  Action: $a"))
      println()
    }
  }
}
