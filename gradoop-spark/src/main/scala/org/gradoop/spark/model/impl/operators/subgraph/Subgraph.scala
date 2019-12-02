package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.types.GveLayoutType

class Subgraph[L <: GveLayoutType] private
(vertexFilterFunction: L#V => Boolean, edgeFilterFunction: L#E => Boolean, strategy: Strategy)
  extends LogicalGraphToLogicalGraphOperator[LogicalGraph[L]] {

  override def execute(graph: LogicalGraph[L]): LogicalGraph[L] = {
    val factory = graph.factory
    import factory.implicits._
    import graph.config.sparkSession.implicits._
    strategy match {
      case Strategy.BOTH =>
        val filteredVertices = graph.vertices.filter(vertexFilterFunction)
        val filteredEdges = graph.edges.filter(edgeFilterFunction)
        graph.factory.init(graph.graphHead, filteredVertices, filteredEdges)

      case Strategy.VERTEX_INDUCED =>
        val filteredVertices = graph.vertices.filter(vertexFilterFunction)
        graph.factory.init(graph.graphHead, filteredVertices, graph.edges).verify // verify induces the edges

      case Strategy.EDGE_INDUCED =>
        val filteredEdges = graph.edges.filter(edgeFilterFunction)
        val inducedVertices = graph.vertices
          .joinWith(filteredEdges, graph.vertices.id isin (filteredEdges.sourceId, filteredEdges.targetId)) // TODO strings -> constants
          .map(t => t._1)
          .dropDuplicates(ColumnNames.ID)
        graph.factory.init(graph.graphHead, inducedVertices, filteredEdges)
    }
  }
}

object Subgraph {

  def both[L <: GveLayoutType]
  (vertexFilterFunction: L#V => Boolean, edgeFilterFunction: L#E => Boolean): Subgraph[L] = {
    new Subgraph(vertexFilterFunction, edgeFilterFunction, Strategy.BOTH)
  }

  def vertexInduced[L <: GveLayoutType](vertexFilterFunction: L#V => Boolean): Subgraph[L] = {
    new Subgraph(vertexFilterFunction, _ => true, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: GveLayoutType](edgeFilterFunction: L#E => Boolean): Subgraph[L] = {
    new Subgraph(_ => true, edgeFilterFunction, Strategy.EDGE_INDUCED)
  }
}
