package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.types.GveGraphLayout

private object Strategy extends Enumeration {
  type Strategy = Value
  val BOTH, VERTEX_INDUCED, EDGE_INDUCED = Value
  // TODO: Add EDGE_INDUCED_PROJECT_FIRST?
}

class Subgraph[L <: GveGraphLayout] private
(vertexFilterFunction: L#V => Boolean, edgeFilterFunction: L#E => Boolean, strategy: Strategy)
  extends LogicalGraphToLogicalGraphOperator[LogicalGraph[L]] {

  override def execute(graph: LogicalGraph[L]): LogicalGraph[L] = {
    val config = graph.config
    import config.implicits._

    strategy match {
      case Strategy.BOTH =>
        val filteredVertices = graph.vertices.filter(vertexFilterFunction)
        val filteredEdges = graph.edges.filter(edgeFilterFunction)
        graph.factory.init(graph.graphHead, filteredVertices, filteredEdges)

      case Strategy.VERTEX_INDUCED =>
        val filteredVertices = graph.vertices.filter(vertexFilterFunction)
        graph.factory.init(graph.graphHead, filteredVertices, graph.edges).verify // verify induces the edges

      case Strategy.EDGE_INDUCED =>
        import graph.config.implicits._
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

  def both[L <: GveGraphLayout]
  (vertexFilterFunction: L#V => Boolean, edgeFilterFunction: L#E => Boolean): Subgraph[L] = {
    new Subgraph(vertexFilterFunction, edgeFilterFunction, Strategy.BOTH)
  }

  def vertexInduced[L <: GveGraphLayout](vertexFilterFunction: L#V => Boolean): Subgraph[L] = {
    new Subgraph(vertexFilterFunction, _ => true, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: GveGraphLayout](edgeFilterFunction: L#E => Boolean): Subgraph[L] = {
    new Subgraph(_ => true, edgeFilterFunction, Strategy.EDGE_INDUCED)
  }
}