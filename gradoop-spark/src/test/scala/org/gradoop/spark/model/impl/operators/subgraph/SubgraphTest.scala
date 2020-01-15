package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.api.config.GradoopSparkConfig

class SubgraphTest extends EpgmGradoopSparkTestBase {
  private val config: GradoopSparkConfig[L] = getConfig
  import config.Implicits._

  describe("SocialNetworkGraph") {
    val loader = getSocialNetworkLoader
    val graph = loader.getLogicalGraph

    describe("Strategy both") {

      describe("vertexFilter = Person, edgeFilter = true") {
        val subgraph = graph
          .subgraph(FilterExpressions.hasLabel("Person"), FilterExpressions.any)

        it("should have 6 vertices") {
          assert(subgraph.vertices.count() == 6)
        }
      }
    }

    describe("Strategy vertex induced") {
      val subgraph = graph
        .vertexInducedSubgraph(FilterExpressions.hasLabel("Person"))

      it("should have 6 vertices") {
        assert(subgraph.vertices.count() == 6)
      }
      it("should have 10 edges") {
        assert(subgraph.edges.count() == 10)
      }
    }

    describe("Strategy edge induced") {
      val subgraph = graph
        .edgeInducedSubgraph(FilterExpressions.hasLabel("hasMember"))

      it("should have 6 vertices") {
        assert(subgraph.vertices.count() == 6)
      }
      it("should have 4 edges") {
        assert(subgraph.edges.count() == 4)
      }
    }
  }
}
