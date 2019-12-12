package org.gradoop.spark.model.impl.operators.subgraph.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.filter.FilterStrings
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.subgraph.Strategy
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.types.Gve

class GveSubgraph[L <: Gve[L]] private
(vertexFilterExpression: String, edgeFilterExpression: String, strategy: Strategy)
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    import graph.config.sparkSession.implicits._
    strategy match {
      case Strategy.BOTH =>
        val filteredVertices = graph.vertices.filter(vertexFilterExpression)
        val filteredEdges = graph.edges.filter(edgeFilterExpression)
        graph.factory.init(graph.graphHead, filteredVertices, filteredEdges)

      case Strategy.VERTEX_INDUCED =>
        val filteredVertices = graph.vertices.filter(vertexFilterExpression)
        graph.factory.init(graph.graphHead, filteredVertices, graph.edges).verify // verify induces the edges

      case Strategy.EDGE_INDUCED =>
        val filteredEdges = graph.edges.filter(edgeFilterExpression)
        val inducedVertices = graph.vertices
          .joinWith(filteredEdges, graph.vertices.id isin (filteredEdges.sourceId, filteredEdges.targetId))
          .map(t => t._1)
          .dropDuplicates(ColumnNames.ID)
        graph.factory.init(graph.graphHead, inducedVertices, filteredEdges)
    }
  }
}

object GveSubgraph {

  def both[L <: Gve[L]](vertexFilterExpression: String, edgeFilterExpression: String): GveSubgraph[L] = {
    new GveSubgraph(vertexFilterExpression, edgeFilterExpression, Strategy.BOTH)
  }

  def vertexInduced[L <: Gve[L]](vertexFilterExpression: String): GveSubgraph[L] = {
    new GveSubgraph(vertexFilterExpression, FilterStrings.any, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: Gve[L]](edgeFilterExpression: String): GveSubgraph[L] = {
    new GveSubgraph(FilterStrings.any, edgeFilterExpression, Strategy.EDGE_INDUCED)
  }
}
