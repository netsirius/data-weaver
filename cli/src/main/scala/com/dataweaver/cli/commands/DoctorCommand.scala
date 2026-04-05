package com.dataweaver.cli.commands

import com.dataweaver.core.config.{SchemaValidator, YAMLParser}
import com.dataweaver.core.dag.DAGResolver

case class DoctorResult(
    yamlValid: Boolean,
    yamlError: Option[String] = None,
    schemaErrors: List[String] = List.empty,
    dagValid: Boolean = false,
    dagLevels: Int = 0,
    dagParallelizable: Int = 0,
    envErrors: List[String] = List.empty,
    javaVersion: String = "",
    javaVersionOk: Boolean = false,
    connectionResults: Map[String, Either[String, Long]] = Map.empty
) {
  def overallHealthy: Boolean =
    yamlValid && schemaErrors.isEmpty && dagValid && envErrors.isEmpty && javaVersionOk
}

object DoctorCommand {

  def run(
      pipelinePath: String,
      connectionsPath: Option[String] = None,
      checkConnections: Boolean = false,
      envProvider: Map[String, String] = sys.env
  ): DoctorResult = {
    val javaVersion = sys.props.getOrElse("java.specification.version", "unknown")
    val javaMajor = try { javaVersion.split('.').last.toInt } catch { case _: Exception => 0 }
    val javaOk = javaMajor >= 17

    val parseResult = YAMLParser.parseFile(pipelinePath)
    parseResult match {
      case Left(err) =>
        val result = DoctorResult(
          yamlValid = false,
          yamlError = Some(err),
          javaVersion = javaVersion,
          javaVersionOk = javaOk
        )
        printReport(result, pipelinePath)
        result

      case Right(config) =>
        val schemaErrors = SchemaValidator.validate(config)

        val (dagValid, dagLevels, dagParallelizable) = try {
          val levels = DAGResolver.resolve(config)
          val parallel = levels.count(_.size > 1)
          (true, levels.size, parallel)
        } catch {
          case _: Exception => (false, 0, 0)
        }

        val envErrors = checkEnvVars(config, envProvider)

        val result = DoctorResult(
          yamlValid = true,
          schemaErrors = schemaErrors,
          dagValid = dagValid,
          dagLevels = dagLevels,
          dagParallelizable = dagParallelizable,
          envErrors = envErrors,
          javaVersion = javaVersion,
          javaVersionOk = javaOk
        )
        printReport(result, pipelinePath)
        result
    }
  }

  private def checkEnvVars(
      config: com.dataweaver.core.config.PipelineConfig,
      envProvider: Map[String, String]
  ): List[String] = {
    val envPattern = """\$\{env\.([^}]+)\}""".r
    val allConfigValues = config.dataSources.flatMap(_.config.values) ++
      config.sinks.flatMap(_.config.values)

    allConfigValues.flatMap { value =>
      envPattern.findAllMatchIn(value).flatMap { m =>
        val varName = m.group(1)
        if (!envProvider.contains(varName))
          Some(s"$${env.$varName} is NOT SET — export $varName=<value> or add to .env file")
        else None
      }
    }.distinct
  }

  private def printReport(result: DoctorResult, path: String): Unit = {
    println()
    println("  Data Weaver Doctor")
    println("  " + "\u2500" * 40)
    println()
    println(s"  Pipeline: $path")

    if (result.yamlValid) println(s"  ${ok(true)} YAML syntax valid")
    else println(s"  ${ok(false)} YAML syntax error: ${result.yamlError.getOrElse("unknown")}")

    if (result.schemaErrors.isEmpty) println(s"  ${ok(true)} Schema valid")
    else result.schemaErrors.foreach(e => println(s"  ${ok(false)} $e"))

    if (result.dagValid)
      println(s"  ${ok(true)} DAG resolved (${result.dagLevels} levels, ${result.dagParallelizable} parallelizable)")
    else println(s"  ${ok(false)} DAG resolution failed")

    println()
    println("  Environment:")
    println(s"  ${ok(result.javaVersionOk)} Java ${result.javaVersion} ${if (result.javaVersionOk) "(meets minimum: 17+)" else "(REQUIRES 17+)"}")

    if (result.envErrors.isEmpty) println(s"  ${ok(true)} All environment variables set")
    else result.envErrors.foreach(e => println(s"  ${ok(false)} $e"))

    val totalErrors = result.schemaErrors.size + result.envErrors.size +
      (if (!result.dagValid) 1 else 0) + (if (!result.javaVersionOk) 1 else 0) +
      (if (!result.yamlValid) 1 else 0)
    println()
    if (totalErrors == 0)
      println("  Summary: All checks passed. Ready to run.")
    else
      println(s"  Summary: $totalErrors error(s) found. Fix them before running weaver apply.")
    println()
  }

  private def ok(pass: Boolean): String = if (pass) "\u2713" else "\u2717"
}
