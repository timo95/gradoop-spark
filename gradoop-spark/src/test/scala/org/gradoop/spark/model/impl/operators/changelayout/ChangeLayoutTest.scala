package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}

class ChangeLayoutTest extends EpgmGradoopSparkTestBase {

  describe("Change LogicalGraph layout") {
    it("Gve to Tfl to Gve", OperatorTest) {
      val gveGraph = getSocialNetworkLoader.getLogicalGraph
      val tflGraph = gveGraph.asTfl(tflConfig)
      val gveGraph2 = tflGraph.asGve(gveConfig)
      assert(gveGraph.equalsByData(gveGraph2))
    }
  }

  describe("Change GraphCollection layout") {
    it("Gve to Tfl to Gve", OperatorTest) {
      val gveCollection = getSocialNetworkLoader.getGraphCollection
      val tflCollection = gveCollection.asTfl(tflConfig)
      val gveCollection2 = tflCollection.asGve(gveConfig)
      assert(gveCollection.equalsByGraphData(gveCollection2))
    }
  }
}
