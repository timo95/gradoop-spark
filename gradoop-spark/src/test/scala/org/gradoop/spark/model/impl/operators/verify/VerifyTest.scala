package org.gradoop.spark.model.impl.operators.verify

import org.gradoop.spark.EpgmGradoopSparkTestBase

class VerifyTest extends EpgmGradoopSparkTestBase {

  describe("verify") {
    val loader = getSocialNetworkLoader
    val graph = loader.getLogicalGraph

    val graph2 = graph.verify
  }
}
