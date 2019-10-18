package org.gradoop.spark.model.api.config

import org.apache.spark.sql.SparkSession
import org.gradoop.common.config.GradoopConfig

abstract class GradoopSparkConfig(implicit spark: SparkSession) extends GradoopConfig {
  // Hold everything as members?
  // Convert to other format by passing config?
}
