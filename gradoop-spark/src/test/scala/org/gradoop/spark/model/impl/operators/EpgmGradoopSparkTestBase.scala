package org.gradoop.spark.model.impl.operators

import org.gradoop.spark.GradoopSparkTestBase
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmTypes

trait EpgmGradoopSparkTestBase extends GradoopSparkTestBase with EpgmTypes {

  private var gveConfig: GradoopSparkConfig[G, V, E, LG, GC] = _

  def getGveConfig: GradoopSparkConfig[G, V, E, LG, GC] = {
    if (gveConfig == null) gveConfig = GradoopSparkConfig.create(EpgmGveLayout, EpgmGveLayout)
    gveConfig
  }

  override def getConfig: GradoopSparkConfig[G, V, E, LG, GC] = getGveConfig
}
