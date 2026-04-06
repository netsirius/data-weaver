package com.dataweaver.cli.commands

import com.dataweaver.core.plugin.{PluginLoader, PluginRegistry}

/** Lists available connectors, transforms, and installed plugins. */
object ListCommand {

  def run(category: String): Unit = {
    // Load external plugins first
    PluginLoader.loadExternalPlugins()

    category match {
      case "connectors" => listConnectors()
      case "transforms" => listTransforms()
      case "plugins"    => listPlugins()
      case "all"        => listConnectors(); println(); listTransforms(); println(); listPlugins()
      case other        =>
        System.err.println(s"Unknown category: '$other'. Use: connectors, transforms, plugins, all")
    }
  }

  private def listConnectors(): Unit = {
    println()
    println("  Source Connectors:")
    println("  " + "\u2500" * 40)
    val sources = PluginRegistry.availableSources.toList.sorted
    if (sources.isEmpty) println("    (none loaded)")
    else sources.foreach(s => println(s"    \u2022 $s"))

    println()
    println("  Sink Connectors:")
    println("  " + "\u2500" * 40)
    val sinks = PluginRegistry.availableSinks.toList.sorted
    if (sinks.isEmpty) println("    (none loaded)")
    else sinks.foreach(s => println(s"    \u2022 $s"))
  }

  private def listTransforms(): Unit = {
    println()
    println("  Transform Types:")
    println("  " + "\u2500" * 40)
    val transforms = PluginRegistry.availableTransforms.toList.sorted
    if (transforms.isEmpty) println("    (none loaded)")
    else transforms.foreach(t => println(s"    \u2022 $t"))
  }

  private def listPlugins(): Unit = {
    val installed = InstallCommand.listInstalled()
    println()
    println("  Installed Plugins:")
    println("  " + "\u2500" * 40)
    if (installed.isEmpty) {
      println("    (none)")
      println()
      println("  Install plugins with:")
      println("    weaver install connector-kafka")
      println("    weaver install /path/to/connector.jar")
    } else {
      installed.foreach(p => println(s"    \u2022 $p"))
    }
  }
}
