package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.ComponentTypes

trait EpgmApp extends ComponentTypes with EpgmConfigs {
  implicit val session: SparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[4]")
    .getOrCreate()
}
