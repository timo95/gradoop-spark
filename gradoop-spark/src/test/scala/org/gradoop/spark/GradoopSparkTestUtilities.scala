package org.gradoop.spark

import org.gradoop.common.GradoopTestUtils
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.SparkAsciiGraphLoader

trait GradoopSparkTestUtilities[L <: Gve[L]] {

  protected def getConfig: GradoopSparkConfig[L]

  protected def getSocialNetworkLoader: SparkAsciiGraphLoader[L] = {
    SparkAsciiGraphLoader.fromStream(getConfig, getClass.getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE))
  }
}
