package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}

class SubgraphTest extends EpgmGradoopSparkTestBase {
  private val config: GradoopSparkConfig[L] = getConfig

  describe("Subgraph") {
    it("SocialNetworkGraph both", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
        "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
        "(eve)-[eka]->(alice)" +
        "(eve)-[ekb]->(bob)" +
        "(frank)-[fkc]->(carol)" +
        "(frank)-[fkd]->(dave)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.subgraph(
        FilterExpressions.hasLabel("Person"),
        FilterExpressions.hasLabel("knows"))

      assert(subgraph.equalsByData(expected))
    }

    it("Tfl SocialNetworkGraph both", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
        "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
        "(eve)-[eka]->(alice)" +
        "(eve)-[ekb]->(bob)" +
        "(frank)-[fkc]->(carol)" +
        "(frank)-[fkd]->(dave)]")

      val graph = loader.getLogicalGraph.toTfl(tflConfig)
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.subgraph(
        FilterExpressions.hasLabel("Person"),
        FilterExpressions.hasLabel("knows"))

      assert(subgraph.toGve(gveConfig).equalsByData(expected))
    }

    it("SocialNetworkGraph both with verify", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
        "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
        "(eve)-[eka]->(alice)" +
        "(eve)-[ekb]->(bob)" +
        "(frank)-[fkc]->(carol)" +
        "(frank)-[fkd]->(dave)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.subgraph(
        FilterExpressions.hasLabel("Person"),
        FilterExpressions.hasLabel("knows"))

      assert(subgraph.equalsByData(expected))
    }

    it("SocialNetworkGraph both only vertices fulfill filter", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(alice)(bob)(carol)(dave)(eve)(frank)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.subgraph(
        FilterExpressions.hasLabel("Person"),
        FilterExpressions.hasLabel("friendOf"))

      assert(subgraph.equalsByData(expected))
    }

    it("SocialNetworkGraph both empty subgraph", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.subgraph(
        FilterExpressions.hasLabel("User"),
        FilterExpressions.hasLabel("friendOf"))

      assert(subgraph.equalsByData(expected))
    }

    it("SocialNetworkGraph vertex induced", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
        "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.vertexInducedSubgraph(
        FilterExpressions.hasLabel("Forum") or FilterExpressions.hasLabel("Tag"))

      assert(subgraph.equalsByData(expected))
    }

    it("SocialNetworkGraph edge induced", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
        "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = graph.edgeInducedSubgraph(
        FilterExpressions.hasLabel("hasTag"))

      assert(subgraph.equalsByData(expected))
    }
  }
}
