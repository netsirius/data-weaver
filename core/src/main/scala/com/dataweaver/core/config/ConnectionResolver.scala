package com.dataweaver.core.config

class ConnectionResolver(
    envProvider: Map[String, String] = sys.env,
    dotEnvPath: String = ".env"
) {

  private val envPattern = """\$\{env\.([^}]+)\}""".r

  private lazy val mergedEnv: Map[String, String] = {
    val dotEnvVars = loadDotEnv(dotEnvPath)
    dotEnvVars ++ envProvider
  }

  def resolve(value: String): String = {
    envPattern.replaceAllIn(
      value,
      m => {
        val varName = m.group(1)
        mergedEnv.getOrElse(
          varName,
          throw new IllegalArgumentException(
            s"Environment variable '$varName' not found. " +
              s"Set it with: export $varName=<value> or add $varName=<value> to .env file"
          )
        )
      }
    )
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
      }
      .toMap
  }
}
