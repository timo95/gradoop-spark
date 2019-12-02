package org.gradoop.spark.model.impl.operators.verify

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.model.api.config.GradoopSparkConfig

class VerifyTest extends EpgmGradoopSparkTestBase {
  private val config: GradoopSparkConfig[L] = getConfig

  describe("verify") {
    val loader = getSocialNetworkLoader
    val graph = loader.logicalGraph

    val graph2 = graph.verify
  }
}
