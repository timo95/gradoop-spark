package org.gradoop.spark

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.{EpgmGveGraphLayout, OldEpgmModel}

trait EpgmGradoopSparkTestBase extends GradoopSparkTestBase[EpgmGveGraphLayout] {
  type L = EpgmGveGraphLayout

  private var gveConfig: Option[GradoopSparkConfig[L]] = None

  protected def getGveConfig: GradoopSparkConfig[L] = {
    if (gveConfig.isEmpty) gveConfig = Some(GradoopSparkConfig.create(EpgmGveLayout, EpgmGveLayout))
    gveConfig.get
  }

  override def getConfig: GradoopSparkConfig[L] = getGveConfig
}
