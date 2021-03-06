package org.gradoop.spark.model.impl.operators.setcollection

import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec

trait DifferenceBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def difference(runDifference: (LGve#GC, LGve#GC) => LGve#GC) {
    it("Overlapping graph collections", OperatorTest) {
      val loader = getSocialNetworkLoader
      val col02 = loader.getGraphCollectionByVariables("g0", "g2")
      val col12 = loader.getGraphCollectionByVariables("g1", "g2")
      val expected = loader.getGraphCollectionByVariables("g0")

      val result = runDifference(col02, col12)
      assert(result.equalsByGraphData(expected))
    }

    it("Non overlapping graph collections", OperatorTest) {
      val loader = getSocialNetworkLoader
      val col01 = loader.getGraphCollectionByVariables("g0", "g1")
      val col23 = loader.getGraphCollectionByVariables("g2", "g3")

      val result = runDifference(col01, col23)
      assert(result.equalsByGraphData(col01))
    }

    it("Total overlapping graph collections", OperatorTest) {
      val loader = getSocialNetworkLoader
      val col01 = loader.getGraphCollectionByVariables("g0", "g1")
      val expected = col01.factory.empty

      val result = runDifference(col01, col01)
      assert(result.equalsByGraphData(expected))
    }
  }
}
