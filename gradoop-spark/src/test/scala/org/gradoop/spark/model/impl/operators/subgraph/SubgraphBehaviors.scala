package org.gradoop.spark.model.impl.operators.subgraph

import org.apache.spark.sql.Column
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.scalatest.FunSpec

trait SubgraphBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def subgraphBoth(runSubgraphBoth: (L#LG, Column, Column) => L#LG) {
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

      val subgraph = runSubgraphBoth(graph,
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

      val subgraph = runSubgraphBoth(graph,
        FilterExpressions.hasLabel("Person"),
        FilterExpressions.hasLabel("friendOf"))

      assert(subgraph.equalsByData(expected))
    }

    it("SocialNetworkGraph both empty subgraph", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = runSubgraphBoth(graph,
        FilterExpressions.hasLabel("User"),
        FilterExpressions.hasLabel("friendOf"))

      assert(subgraph.equalsByData(expected))
    }
  }

  def subgraphVertexInduced(runSubgraphVertexInduced: (L#LG, Column) => L#LG) {
    it("SocialNetworkGraph vertex induced", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
        "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = runSubgraphVertexInduced(graph,
        FilterExpressions.hasLabel("Forum") or FilterExpressions.hasLabel("Tag"))

      assert(subgraph.equalsByData(expected))
    }
  }

  def subgraphEdgeInduced(runSubgraphEdgeInduced: (L#LG, Column) => L#LG) {
    it("SocialNetworkGraph edge induced", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:_DB[" +
        "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
        "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)]")

      val graph = loader.getLogicalGraph
      val expected = loader.getLogicalGraphByVariable("expected")

      val subgraph = runSubgraphEdgeInduced(graph, FilterExpressions.hasLabel("hasTag"))

      assert(subgraph.equalsByData(expected))
    }
  }
}
