package org.gradoop.spark.model.impl.operators.transformation

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.transformation.TransformationFunctions

class TransformationTest extends EpgmGradoopSparkTestBase {

  describe("Transformation") {
    describe("Rename Label") {
      it("do") {
        val config = getConfig

        import config.logicalGraphFactory.Implicits._

        val graph = getSocialNetworkLoader.getLogicalGraph

        val graph2 = graph.transformVertices(
          TransformationFunctions.renameLabel("Person", "d"))
      }
    }

  }
}
