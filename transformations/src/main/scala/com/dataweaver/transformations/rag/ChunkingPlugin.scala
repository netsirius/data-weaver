package com.dataweaver.transformations.rag

import com.dataweaver.core.plugin.{TransformConfig, TransformPlugin}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Splits text documents into chunks for RAG processing.
  *
  * Config (via extra map):
  *   strategy    - fixed | sentence | recursive (default: fixed)
  *   size        - chunk size in characters (default: 512)
  *   overlap     - overlap between chunks in characters (default: 50)
  *   textColumn  - column containing text to chunk (default: "text")
  *   idColumn    - column for document ID (default: "id")
  */
class ChunkingPlugin extends TransformPlugin {
  private val logger = LogManager.getLogger(getClass)

  def transformType: String = "Chunking"

  def transform(inputs: Map[String, DataFrame], config: TransformConfig)(implicit
      spark: SparkSession
  ): DataFrame = {
    val df = inputs.values.headOption.getOrElse(
      throw new IllegalArgumentException(s"Chunking '${config.id}' requires input"))

    val strategy = config.extra.getOrElse("strategy", "fixed")
    val chunkSize = config.extra.getOrElse("size", "512").toInt
    val overlap = config.extra.getOrElse("overlap", "50").toInt
    val textColumn = config.extra.getOrElse("textColumn", "text")
    val idColumn = config.extra.getOrElse("idColumn", "id")

    import spark.implicits._

    // Collect and chunk locally (chunking is a CPU operation, not data-parallel)
    val chunked = df.select(col(idColumn), col(textColumn)).collect().flatMap { row =>
      val docId = row.get(0).toString
      val text = Option(row.get(1)).map(_.toString).getOrElse("")

      val chunks = strategy match {
        case "sentence" => chunkBySentence(text, chunkSize, overlap)
        case "recursive" => chunkRecursive(text, chunkSize, overlap)
        case _ => chunkFixed(text, chunkSize, overlap) // "fixed"
      }

      chunks.zipWithIndex.map { case (chunk, idx) =>
        (docId, s"${docId}_chunk_$idx", idx, chunk, chunk.length)
      }
    }

    chunked.toSeq.toDF("doc_id", "chunk_id", "chunk_index", "text", "chunk_length")
  }

  /** Fixed-size chunking with overlap. */
  private def chunkFixed(text: String, size: Int, overlap: Int): List[String] = {
    if (text.isEmpty) return List.empty
    val step = math.max(1, size - overlap)
    (0 until text.length by step)
      .map(i => text.substring(i, math.min(i + size, text.length)))
      .filter(_.nonEmpty)
      .toList
  }

  /** Sentence-based chunking — split on sentence boundaries, group up to size. */
  private def chunkBySentence(text: String, size: Int, overlap: Int): List[String] = {
    val sentences = text.split("(?<=[.!?])\\s+").toList
    val chunks = scala.collection.mutable.ListBuffer[String]()
    var current = new StringBuilder()

    sentences.foreach { sentence =>
      if (current.length + sentence.length > size && current.nonEmpty) {
        chunks += current.toString.trim
        // Keep overlap from end of current chunk
        val overlapText = current.toString.takeRight(overlap)
        current = new StringBuilder(overlapText)
      }
      current.append(sentence).append(" ")
    }
    if (current.nonEmpty) chunks += current.toString.trim

    chunks.toList
  }

  /** Recursive chunking — try paragraph splits first, then sentences, then fixed. */
  private def chunkRecursive(text: String, size: Int, overlap: Int): List[String] = {
    if (text.length <= size) return List(text)

    // Try splitting by paragraphs
    val paragraphs = text.split("\n\n+").toList
    if (paragraphs.size > 1) {
      return paragraphs.flatMap(p => chunkRecursive(p, size, overlap))
    }

    // Fall back to sentence splitting
    val sentenceChunks = chunkBySentence(text, size, overlap)
    if (sentenceChunks.forall(_.length <= size * 1.2)) return sentenceChunks

    // Last resort: fixed size
    chunkFixed(text, size, overlap)
  }
}
