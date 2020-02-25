package org.gradoop.spark

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.EpgmGve
import org.gradoop.spark.util.EpgmConfigs

trait EpgmGradoopSparkTestBase extends GradoopSparkTestBase[EpgmGve] with EpgmConfigs {

  override protected def getConfig: GradoopSparkConfig[LGve] = gveConfig
}
