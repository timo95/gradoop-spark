package org.gradoop.spark.model.impl.operators

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.functions.filter.HasLabel

class SubgraphTest extends EpgmGradoopSparkTestBase {

  describe("SocialNetworkGraph") {
    val loader = getSocialNetworkLoader
    val graph = loader.logicalGraph

    describe("Strategy both") {

      describe("vertexFilter = Person, edgeFilter = true") {
        val subgraph = graph.subgraph(new HasLabel("Person").call, _ => true)

        it("should have 6 vertices") {
          assert(subgraph.vertices.count() == 6)
        }
      }
    }

    describe("Strategy vertex induced") {
      val subgraph = graph.vertexInducedSubgraph(e => e.labels.contains("Person"))

      it("should have 6 vertices") {
        assert(subgraph.vertices.count() == 6)
      }
      it("should have 10 edges") {
        assert(subgraph.edges.count() == 10)
      }
    }

    describe("Strategy edge induced") {
      val subgraph = graph.edgeInducedSubgraph(e => e.labels.contains("hasMember"))

      it("should have 6 vertices") {
        assert(subgraph.vertices.count() == 6)
      }
      it("should have 4 edges") {
        assert(subgraph.edges.count() == 4)
      }
    }
  }
}
