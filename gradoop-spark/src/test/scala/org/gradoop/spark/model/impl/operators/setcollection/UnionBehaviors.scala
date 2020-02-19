package org.gradoop.spark.model.impl.operators.setcollection

import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec

trait UnionBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def union(runUnion: (LGve#GC, LGve#GC) => LGve#GC) {
    it("Overlapping graph collections", OperatorTest) {
      val loader = getSocialNetworkLoader
      val col02 = loader.getGraphCollectionByVariables("g0", "g2")
      val col12 = loader.getGraphCollectionByVariables("g1", "g2")
      val expected = loader.getGraphCollectionByVariables("g0", "g1", "g2")

      val result = runUnion(col02, col12)
      assert(result.equalsByGraphData(expected))
    }

    it("Non overlapping graph collections", OperatorTest) {
      val loader = getSocialNetworkLoader
      val col01 = loader.getGraphCollectionByVariables("g0", "g1")
      val col23 = loader.getGraphCollectionByVariables("g2", "g3")
      val expected = loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3")

      val result = runUnion(col01, col23)
      assert(result.equalsByGraphData(expected))
    }

    it("Total overlapping graph collections", OperatorTest) {
      val loader = getSocialNetworkLoader
      val col01 = loader.getGraphCollectionByVariables("g0", "g1")

      val result = runUnion(col01, col01)
      assert(result.equalsByGraphData(col01))
    }
  }
}
