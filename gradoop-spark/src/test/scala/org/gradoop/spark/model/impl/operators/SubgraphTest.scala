package org.gradoop.spark.model.impl.operators

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.functions.filter.HasLabel

class SubgraphTest extends EpgmGradoopSparkTestBase {


  describe("SocialNetworkGraph") {
    val loader = getSocialNetworkLoader
    val graph = loader.getLogicalGraph

    describe("Strategy BOTH") {

      describe("vertexFilter = Person, edgeFilter = true") {
        val subgraph = graph.subgraph(new HasLabel("Person"), _ => true)

        it("should have 6 vertices") {
          assert(subgraph.getVertices.count() == 6)
        }
      }
    }
  }
}