package com.dataweaver

import org.apache.spark.SparkConf

package object config {

  val SPARK_CONF: SparkConf = {
    val conf = new SparkConf()
    val options = Map(
      "spark.worker.cleanup.enabled" -> "true",
      "spark.shuffle.consolidateFiles" -> "true",
      "spark.driver.memory" -> "1g",
      "spark.executor.memory" -> "1g",
      "spark.executor.cores" -> "2",
      "spark.default.parallelism" -> "200",
      "spark.sql.shuffle.partitions" -> "200",
      "spark.shuffle.service.enabled" -> "false",
      "spark.network.timeout" -> "120s",
      "spark.ui.retainedJobs" -> "300",
      "spark.ui.retainedStages" -> "400",
      "spark.ui.retainedTasks" -> "1000",
      "spark.ui.retainedDeadExecutors" -> "10",
      "spark.sql.warehouse.dir" -> "file:/tmp/spark-warehouse",
      "spark.local.dir" -> "/tmp",
      // GCP config:
      "spark.hadoop.google.cloud.auth.service.account.enable" -> "true",
      "fs.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
      "fs.AbstractFileSystem.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    options.foreach { case (k, v) =>
      conf.set(k, v)
    }
    conf
  }
}
