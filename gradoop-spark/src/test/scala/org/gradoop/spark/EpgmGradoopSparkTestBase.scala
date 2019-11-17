package org.gradoop.spark

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.gve.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmGveLayoutType

trait EpgmGradoopSparkTestBase extends GradoopSparkTestBase[EpgmGveLayoutType] {
  type L = EpgmGveLayoutType

  private var gveConfig: Option[GradoopSparkConfig[L]] = None

  protected def getGveConfig: GradoopSparkConfig[L] = {
    if (gveConfig.isEmpty) gveConfig = Some(GradoopSparkConfig.create(EpgmGveLayout, EpgmGveLayout))
    gveConfig.get
  }

  override def getConfig: GradoopSparkConfig[L] = getGveConfig
}
