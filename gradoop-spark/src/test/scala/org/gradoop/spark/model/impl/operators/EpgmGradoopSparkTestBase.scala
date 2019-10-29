package org.gradoop.spark.model.impl.operators

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.GradoopSparkTestBase
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmTypes

class EpgmGradoopSparkTestBase extends GradoopSparkTestBase with EpgmTypes {

  implicit val session: SparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[4]")
    .getOrCreate()

  private var gveConfig: GradoopSparkConfig[G, V, E, LG, GC] = _

  def getGveConfig: GradoopSparkConfig[G, V, E, LG, GC] = {
    if (gveConfig == null) gveConfig = GradoopSparkConfig.create(EpgmGveLayout, EpgmGveLayout)
    gveConfig
  }

  override def getConfig: GradoopSparkConfig[G, V, E, LG, GC] = getGveConfig
}
