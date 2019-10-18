package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.epgm.Epgm

case class EpgmGradoopSparkConfig(implicit session: SparkSession) extends GradoopSparkConfig with Epgm {

}
