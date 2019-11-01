package org.gradoop.spark.model.impl.operators

import org.gradoop.spark.GradoopSparkTestBase
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmTypes

trait EpgmGradoopSparkTestBase extends GradoopSparkTestBase with EpgmTypes {

  private var gveConfig: Option[GradoopSparkConfig[G, V, E, LG, GC]] = None

  protected def getGveConfig: GradoopSparkConfig[G, V, E, LG, GC] = {
    if (gveConfig.isEmpty) gveConfig = Some(GradoopSparkConfig.create(EpgmGveLayout, EpgmGveLayout))
    gveConfig.get
  }

  override def getConfig: GradoopSparkConfig[G, V, E, LG, GC] = getGveConfig
}
