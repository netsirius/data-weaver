package com.dataweaver.core.config

object SchemaValidator {

  def validate(config: PipelineConfig): List[String] = {
    var errors = List.empty[String]

    if (config.name.isEmpty)
      errors :+= "Pipeline name cannot be empty"

    val sourceIds = config.dataSources.map(_.id)
    val transformIds = config.transformations.map(_.id)
    val allIds = sourceIds ++ transformIds

    val duplicates = allIds.groupBy(identity).collect { case (id, list) if list.size > 1 => id }
    duplicates.foreach { id =>
      errors :+= s"Duplicate id '$id' — all dataSource and transformation ids must be unique"
    }

    config.transformations.foreach { t =>
      t.sources.foreach { srcId =>
        if (!allIds.contains(srcId))
          errors :+= s"Transform '${t.id}' references source '$srcId' which does not exist. " +
            s"Available: ${allIds.mkString(", ")}"
      }
    }

    config.sinks.foreach { sink =>
      sink.source match {
        case Some(srcId) if !allIds.contains(srcId) =>
          errors :+= s"Sink '${sink.id}' references source '$srcId' which does not exist. " +
            s"Available: ${allIds.mkString(", ")}"
        case None if config.transformations.nonEmpty =>
          errors :+= s"Sink '${sink.id}' has no 'source' field. " +
            s"Each sink must declare which transform it reads from. " +
            s"Available: ${transformIds.mkString(", ")}"
        case _ =>
      }
    }

    errors
  }
}
