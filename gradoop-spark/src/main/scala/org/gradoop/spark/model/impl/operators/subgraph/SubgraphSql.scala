package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.functions.filter.FilterStrings
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.types.GveGraphLayout

class SubgraphSql[L <: GveGraphLayout] private
(vertexFilterExpression: String, edgeFilterExpression: String, strategy: Strategy)
  extends LogicalGraphToLogicalGraphOperator[LogicalGraph[L]] {

  override def execute(graph: LogicalGraph[L]): LogicalGraph[L] = {
    strategy match {
      case Strategy.BOTH =>
        val filteredVertices = graph.vertices.filter(vertexFilterExpression)
        val filteredEdges = graph.edges.filter(edgeFilterExpression)
        graph.factory.init(graph.graphHead, filteredVertices, filteredEdges)

      case Strategy.VERTEX_INDUCED =>
        val filteredVertices = graph.vertices.filter(vertexFilterExpression)
        graph.factory.init(graph.graphHead, filteredVertices, graph.edges).verify // verify induces the edges

      case Strategy.EDGE_INDUCED =>
        import graph.config.implicits._
        val filteredEdges = graph.edges.filter(edgeFilterExpression)
        val inducedVertices = graph.vertices
          .joinWith(filteredEdges, graph.vertices.id isin (filteredEdges.sourceId, filteredEdges.targetId)) // TODO strings -> constants
          .map(t => t._1)
          .dropDuplicates(ColumnNames.ID)
        graph.factory.init(graph.graphHead, inducedVertices, filteredEdges)
    }
  }
}

object SubgraphSql {

  def both[L <: GveGraphLayout]
  (vertexFilterExpression: String, edgeFilterExpression: String): SubgraphSql[L] = {
    new SubgraphSql(vertexFilterExpression, edgeFilterExpression, Strategy.BOTH)
  }

  def vertexInduced[L <: GveGraphLayout](vertexFilterExpression: String): SubgraphSql[L] = {
    new SubgraphSql(vertexFilterExpression, FilterStrings.any, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: GveGraphLayout](edgeFilterExpression: String): SubgraphSql[L] = {
    new SubgraphSql(FilterStrings.any, edgeFilterExpression, Strategy.EDGE_INDUCED)
  }
}