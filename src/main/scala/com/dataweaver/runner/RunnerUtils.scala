package com.dataweaver.runner

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.util.jar.{JarEntry, JarInputStream, JarOutputStream}
import scala.collection.compat.immutable.LazyList

object RunnerUtils {

  /** Get filtered pipeline files from the specified folder based on tag and regex patterns.
    *
    * @param pipelinesFolder
    *   The folder containing pipeline files.
    * @param tag
    *   An optional tag to filter pipeline files.
    * @param regex
    *   An optional regex pattern to filter pipeline files.
    * @return
    *   A sequence of filtered pipeline files.
    */
  private[runner] def getFilteredPipelineFiles(
      pipelinesFolder: String,
      tag: Option[String],
      regex: Option[String]
  ): Seq[File] = {
    val pipelineDirectory = new File(pipelinesFolder)
    val allFiles = pipelineDirectory.listFiles()

    // Filter by tag if provided
    val taggedFiles = tag match {
      case Some(tagValue) => allFiles.filter(file => file.getName.contains(tagValue))
      case None           => allFiles
    }

    // Filter by regex if provided
    val filteredFiles = regex match {
      case Some(regexPattern) => taggedFiles.filter(file => file.getName.matches(regexPattern))
      case None               => taggedFiles
    }

    filteredFiles
  }

  private[runner] def getConfigFiles(configFolder: String): Seq[File] = {
    val configDirectory = new File(configFolder)
    configDirectory.listFiles()
  }

  /** Add the specified files to the JAR.
    *
    * @param jarPath
    *   The path to the JAR file.
    * @param destinationFolder
    *   The destination folder inside the JAR.
    * @param files
    *   The files to add to the JAR.
    */
  private[runner] def addFilesToJar(
      jarPath: String,
      destinationFolder: String,
      filesToAdd: Seq[File]
  ): Unit = {
    val tempJarPath = jarPath + ".tmp"
    val jarInputStream = new JarInputStream(new FileInputStream(jarPath))
    val jarOutputStream = new JarOutputStream(new FileOutputStream(tempJarPath))

    // Copy existing entries from the original JAR
    LazyList.continually(jarInputStream.getNextJarEntry).takeWhile(_ != null).foreach { jarEntry =>
      jarOutputStream.putNextEntry(new JarEntry(jarEntry.getName))

      val buffer = new Array[Byte](1024)
      var len = jarInputStream.read(buffer)
      while (len != -1) {
        jarOutputStream.write(buffer, 0, len)
        len = jarInputStream.read(buffer)
      }
      jarInputStream.closeEntry()
      jarOutputStream.closeEntry()
    }

    // Add new files
    filesToAdd.foreach { file =>
      val entryName = s"$destinationFolder/${file.getName}"
      val entry = new JarEntry(entryName)
      jarOutputStream.putNextEntry(entry)

      file.getName match {
        case "application.conf" =>
          val config = ConfigFactory.parseFile(file)
          val updatedConfig = config.withValue(
            "weaver.pipeline.pipelines_dir",
            ConfigValueFactory.fromAnyRef(destinationFolder)
          )
          val updatedConfigStr = updatedConfig.root().render(ConfigRenderOptions.defaults())
          val byteStream = new ByteArrayInputStream(updatedConfigStr.getBytes)
          byteStream.transferTo(jarOutputStream)
          byteStream.close()
        case _ =>
          val fileStream = new FileInputStream(file)
          fileStream.transferTo(jarOutputStream)
          fileStream.close()
      }

      jarOutputStream.closeEntry()
    }

    jarInputStream.close()
    jarOutputStream.close()

    // Replace original JAR with the temporary one
    new File(jarPath).delete()
    new File(tempJarPath).renameTo(new File(jarPath))
    new File(tempJarPath).delete()

  }
}
