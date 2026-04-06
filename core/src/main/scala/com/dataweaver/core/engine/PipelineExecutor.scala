package com.dataweaver.core.engine

import com.dataweaver.core.config._
import com.dataweaver.core.dag.{DAGResolver, SourceNode, TransformNode}
import com.dataweaver.core.plugin.{PluginRegistry, TransformConfig}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object PipelineExecutor {

  private val logger = LogManager.getLogger(getClass)

  def execute(config: PipelineConfig)(implicit spark: SparkSession): Unit = {
    logger.info(s"Starting pipeline '${config.name}'")

    val errors = SchemaValidator.validate(config)
    if (errors.nonEmpty) {
      errors.foreach(e => logger.error(s"Validation error: $e"))
      throw new IllegalArgumentException(s"Pipeline validation failed:\n${errors.mkString("\n")}")
    }

    val levels = DAGResolver.resolve(config)
    logger.info(s"DAG resolved: ${levels.size} execution levels")

    val results = scala.collection.mutable.Map[String, DataFrame]()

    levels.foreach { level =>
      val futures = level.map {
        case SourceNode(srcConfig) =>
          Future {
            logger.info(s"Reading source '${srcConfig.id}' (${srcConfig.`type`})")
            val connector = PluginRegistry
              .getSource(srcConfig.`type`)
              .getOrElse(throw new IllegalArgumentException(
                s"Unknown source type '${srcConfig.`type`}'. " +
                  s"Available: ${PluginRegistry.availableSources.mkString(", ")}"
              ))
            val enrichedConfig = srcConfig.config ++ Map("id" -> srcConfig.id, "query" -> srcConfig.query).filter(_._2.nonEmpty)
            val df = connector.read(enrichedConfig)
            (srcConfig.id, df)
          }

        case TransformNode(tConfig) =>
          Future {
            logger.info(s"Applying transform '${tConfig.id}' (${tConfig.`type`})")
            val plugin = PluginRegistry
              .getTransform(tConfig.`type`)
              .getOrElse(throw new IllegalArgumentException(
                s"Unknown transform type '${tConfig.`type`}'. " +
                  s"Available: ${PluginRegistry.availableTransforms.mkString(", ")}"
              ))
            val inputs = tConfig.sources.map { srcId =>
              results.synchronized {
                srcId -> results.getOrElse(srcId,
                  throw new IllegalStateException(s"DataFrame for '$srcId' not found"))
              }
            }.toMap
            val transformConfig = TransformConfig(
              id = tConfig.id,
              sources = tConfig.sources,
              query = tConfig.query,
              action = tConfig.action,
              extra = tConfig.config
            )
            val df = plugin.transform(inputs, transformConfig)
            (tConfig.id, df)
          }
      }

      val levelResults = futures.map(f => Await.result(f, 30.minutes))
      results.synchronized {
        levelResults.foreach { case (id, df) => results += (id -> df) }
      }

      logger.info(s"Level completed: ${level.map(_.id).mkString(", ")}")
    }

    config.sinks.foreach { sinkConfig =>
      val sourceId = sinkConfig.source.getOrElse(
        config.dataSources.last.id
      )
      val df = results.getOrElse(sourceId,
        throw new IllegalStateException(
          s"Sink '${sinkConfig.id}' references source '$sourceId' but no DataFrame found"
        ))

      logger.info(s"Writing to sink '${sinkConfig.id}' (${sinkConfig.`type`}) from '$sourceId'")
      val connector = PluginRegistry
        .getSink(sinkConfig.`type`)
        .getOrElse(throw new IllegalArgumentException(
          s"Unknown sink type '${sinkConfig.`type`}'. " +
            s"Available: ${PluginRegistry.availableSinks.mkString(", ")}"
        ))
      connector.write(df, config.name, sinkConfig.config)
      logger.info(s"Sink '${sinkConfig.id}' completed")
    }

    logger.info(s"Pipeline '${config.name}' completed successfully")
  }

  /** Execute pipeline and return all intermediate DataFrames (for testing).
    * Same as execute() but returns the results map instead of only writing to sinks.
    */
  def executeAndCapture(config: PipelineConfig)(implicit spark: SparkSession): Map[String, DataFrame] = {
    logger.info(s"Starting pipeline '${config.name}' (capture mode)")

    val errors = SchemaValidator.validate(config)
    if (errors.nonEmpty) {
      throw new IllegalArgumentException(s"Pipeline validation failed:\n${errors.mkString("\n")}")
    }

    val levels = DAGResolver.resolve(config)
    val results = scala.collection.mutable.Map[String, DataFrame]()

    levels.foreach { level =>
      val futures = level.map {
        case SourceNode(srcConfig) =>
          Future {
            val connector = PluginRegistry
              .getSource(srcConfig.`type`)
              .getOrElse(throw new IllegalArgumentException(
                s"Unknown source type '${srcConfig.`type`}'"))
            (srcConfig.id, connector.read(srcConfig.config))
          }

        case TransformNode(tConfig) =>
          Future {
            val plugin = PluginRegistry
              .getTransform(tConfig.`type`)
              .getOrElse(throw new IllegalArgumentException(
                s"Unknown transform type '${tConfig.`type`}'"))
            val inputs = tConfig.sources.map { srcId =>
              results.synchronized {
                srcId -> results.getOrElse(srcId,
                  throw new IllegalStateException(s"DataFrame for '$srcId' not found"))
              }
            }.toMap
            val transformConfig = TransformConfig(
              id = tConfig.id,
              sources = tConfig.sources,
              query = tConfig.query,
              action = tConfig.action,
              extra = tConfig.config
            )
            (tConfig.id, plugin.transform(inputs, transformConfig))
          }
      }

      val levelResults = futures.map(f => Await.result(f, 30.minutes))
      results.synchronized {
        levelResults.foreach { case (id, df) => results += (id -> df) }
      }
    }

    // Also capture sink outputs (use same source mapping)
    config.sinks.foreach { sinkConfig =>
      val sourceId = sinkConfig.source.getOrElse(config.dataSources.last.id)
      results.get(sourceId).foreach { df =>
        results += (sinkConfig.id -> df)
      }
    }

    results.toMap
  }
}
