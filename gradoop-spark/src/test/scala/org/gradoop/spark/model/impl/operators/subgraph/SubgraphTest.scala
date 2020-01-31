package org.gradoop.spark.model.impl.operators.subgraph

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.operators.subgraph.gve.GveSubgraph
import org.gradoop.spark.model.impl.operators.subgraph.tfl.TflSubgraph
import org.gradoop.spark.model.impl.types.EpgmTfl

class SubgraphTest extends SubgraphBehaviors {

  describe("GveSubgrah") {
    it should behave like subgraphBoth(runGveSubgraph(_, _, _, Strategy.BOTH))
    it should behave like subgraphVertexInduced(runGveSubgraph(_, _, FilterExpressions.any, Strategy.VERTEX_INDUCED))
    it should behave like subgraphEdgeInduced(runGveSubgraph(_, FilterExpressions.any, _, Strategy.EDGE_INDUCED))
  }

  describe("TflSubgraph") {
    it should behave like subgraphBoth(runTflSubgraph(_, _, _, Strategy.BOTH))
    it should behave like subgraphVertexInduced(runTflSubgraph(_, _, FilterExpressions.any, Strategy.VERTEX_INDUCED))
    it should behave like subgraphEdgeInduced(runTflSubgraph(_, FilterExpressions.any, _, Strategy.EDGE_INDUCED))
  }

  def runGveSubgraph(graph: L#LG, vertexFilterExpression: Column, edgeFilterExpression: Column,
    strategy: Strategy): L#LG = {
    graph.callForGraph(new GveSubgraph[L](vertexFilterExpression, edgeFilterExpression, strategy))
  }

  def runTflSubgraph(graph: L#LG, vertexFilterExpression: Column, edgeFilterExpression: Column,
    strategy: Strategy): L#LG = {
    graph.asTfl(tflConfig)
      .callForGraph(new TflSubgraph[EpgmTfl](vertexFilterExpression, edgeFilterExpression, strategy))
      .asGve(graph.config)
  }
}
