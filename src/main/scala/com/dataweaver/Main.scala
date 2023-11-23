package com.dataweaver

import com.dataweaver.core.DataFlowManager
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val tag = if (args.nonEmpty) Some(args(0)) else None
    val regex = if (args.nonEmpty) Some(args(1)) else None

    val spark = SparkSession.builder
      .getOrCreate()

    try {
      println(s"Running DataWeaver with tag: $tag and regex: $regex")
      val manager = new DataFlowManager()
      manager.executeDataFlows(tag, regex)
    } finally {
      spark.stop()
    }
  }
}
