package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}

class ChangeLayoutTest extends EpgmGradoopSparkTestBase {

  describe("Change layout") {
    it("Gve to Tfl to Gve", OperatorTest) {
      val gveGraph = getSocialNetworkLoader.getLogicalGraph
      val tflGraph = gveGraph.toTfl(tflConfig)
      val gveGraph2 = tflGraph.toGve(gveConfig)
      assert(gveGraph.equalsByData(gveGraph2))
    }
  }
}
