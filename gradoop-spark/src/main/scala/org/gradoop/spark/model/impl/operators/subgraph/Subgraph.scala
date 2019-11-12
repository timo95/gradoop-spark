package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy

private object Strategy extends Enumeration {
  type Strategy = Value
  val BOTH, VERTEX_INDUCED, EDGE_INDUCED = Value
  // TODO: Add EDGE_INDUCED_PROJECT_FIRST?
}

class Subgraph[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]] private
(vertexFilterFunction: V => Boolean, edgeFilterFunction: E => Boolean, strategy: Strategy)
  extends LogicalGraphToLogicalGraphOperator[LG] {

  override def execute(graph: LG): LG = strategy match {
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

object Subgraph {

  def both[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]]
  (vertexFilterFunction: V => Boolean, edgeFilterFunction: E => Boolean): Subgraph[G, V, E, LG, GC] = {
    new Subgraph(vertexFilterFunction, edgeFilterFunction, Strategy.BOTH)
  }

  def vertexInduced[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]](vertexFilterFunction: V => Boolean): Subgraph[G, V, E, LG, GC] = {
    new Subgraph(vertexFilterFunction, _ => true, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]](edgeFilterFunction: E => Boolean): Subgraph[G, V, E, LG, GC] = {
    new Subgraph(_ => true, edgeFilterFunction, Strategy.EDGE_INDUCED)
  }
}