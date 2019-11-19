package org.gradoop.spark

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.EpgmGveLayoutType
import org.gradoop.spark.util.EpgmConfigs

trait EpgmGradoopSparkTestBase extends GradoopSparkTestBase[EpgmGveLayoutType] with EpgmConfigs {

  override def getConfig: GradoopSparkConfig[L] = gveConfig
}
