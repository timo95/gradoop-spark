package org.gradoop.spark.model.impl.operators.subgraph.tfl

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.subgraph.Strategy
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.types.Tfl

class TflSubgraph[L <: Tfl[L]] private
(vertexFilterExpression: Column, edgeFilterExpression: Column, strategy: Strategy)
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    strategy match {
      case Strategy.BOTH =>
        val vertexProp = graph.vertexProperties
        val vertices = graph.vertices.map(v => (v._1, v._2.join(vertexProp(v._1))))
          .mapValues(_.filter(vertexFilterExpression))
        val resVert = vertices.mapValues(_.as[L#V])
        val resVertProp = vertices.mapValues(_.as[L#P])

        val edgeProp = graph.edgeProperties
        val edges = graph.edges.map(v => (v._1, v._2.join(edgeProp(v._1))))
          .mapValues(_.filter(edgeFilterExpression))
        val resEdge = edges.mapValues(_.as[L#E])
        val resEdgeProp = edges.mapValues(_.as[L#P])
        graph.factory.init(graph.graphHead, resVert, resEdge, graph.graphHeadProperties, resVertProp, resEdgeProp)

      case Strategy.VERTEX_INDUCED =>
        graph // TODO
      case Strategy.EDGE_INDUCED =>
        graph
    }
  }
}

object TflSubgraph {

  def both[L <: Tfl[L]](vertexFilterExpression: Column, edgeFilterExpression: Column): TflSubgraph[L] = {
    new TflSubgraph(vertexFilterExpression, edgeFilterExpression, Strategy.BOTH)
  }

  def vertexInduced[L <: Tfl[L]](vertexFilterExpression: Column): TflSubgraph[L] = {
    new TflSubgraph(vertexFilterExpression, FilterExpressions.any, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: Tfl[L]](edgeFilterExpression: Column): TflSubgraph[L] = {
    new TflSubgraph(FilterExpressions.any, edgeFilterExpression, Strategy.EDGE_INDUCED)
  }
}
