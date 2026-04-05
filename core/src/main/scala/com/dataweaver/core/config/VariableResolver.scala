package com.dataweaver.core.config

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class VariableResolver(
    envProvider: Map[String, String] = sys.env,
    dotEnvPath: String = ".env",
    dateProvider: () => LocalDate = () => LocalDate.now()
) {

  private val envPattern = """\$\{env\.([^}]+)\}""".r
  private val dateTodayPattern = """\$\{date\.today\}""".r
  private val dateYesterdayPattern = """\$\{date\.yesterday\}""".r
  private val dateOffsetPattern = """\$\{date\.offset\((-?\d+)\)\}""".r
  private val dateFormatPattern = """\$\{date\.format\('([^']+)'\)\}""".r

  private lazy val mergedEnv: Map[String, String] = {
    val dotEnvVars = loadDotEnv(dotEnvPath)
    dotEnvVars ++ envProvider
  }

  def resolve(value: String): String = {
    var result = value
    result = envPattern.replaceAllIn(result, m => {
      val varName = m.group(1)
      java.util.regex.Matcher.quoteReplacement(
        mergedEnv.getOrElse(varName,
          throw new IllegalArgumentException(
            s"Environment variable '$varName' not found. " +
              s"Set it with: export $varName=<value> or add to .env file"))
      )
    })
    val today = dateProvider()
    result = dateTodayPattern.replaceAllIn(result, today.toString)
    result = dateYesterdayPattern.replaceAllIn(result, today.minusDays(1).toString)
    result = dateOffsetPattern.replaceAllIn(result, m => {
      val days = m.group(1).toInt
      today.plusDays(days).toString
    })
    result = dateFormatPattern.replaceAllIn(result, m => {
      val pattern = m.group(1)
      today.format(DateTimeFormatter.ofPattern(pattern))
    })
    result
  }

  def resolveMap(config: Map[String, String]): Map[String, String] =
    config.map { case (k, v) => k -> resolve(v) }

  private def loadDotEnv(path: String): Map[String, String] = {
    val file = new java.io.File(path)
    if (!file.exists()) return Map.empty
    scala.io.Source.fromFile(file).getLines()
      .map(_.trim)
      .filterNot(line => line.isEmpty || line.startsWith("#"))
      .flatMap { line =>
        line.split("=", 2) match {
          case Array(key, value) => Some(key.trim -> value.trim)
          case _                 => None
        }
      }.toMap
  }
}
