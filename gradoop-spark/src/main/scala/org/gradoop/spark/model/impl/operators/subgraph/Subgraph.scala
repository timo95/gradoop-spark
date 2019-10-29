package org.gradoop.spark.model.impl.operators.subgraph

import org.apache.spark.api.java.function.FilterFunction
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator

class Subgraph[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(vertexFilterFunction: FilterFunction[V], edgeFilterFunction: FilterFunction[E])
  extends LogicalGraphToLogicalGraphOperator[LG] {

  override def execute(graph: LG): LG = {
    graph.getFactory.init(graph.getGraphHead,
      graph.getVertices.filter(vertexFilterFunction),
      graph.getEdges.filter(edgeFilterFunction))
  }
}
