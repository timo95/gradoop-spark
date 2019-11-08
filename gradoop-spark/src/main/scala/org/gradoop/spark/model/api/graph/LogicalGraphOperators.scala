package org.gradoop.spark.model.api.graph

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.impl.operators.subgraph.Subgraph

trait LogicalGraphOperators[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]] {
  this: LG =>

  def subgraph(vertexFilterFunction: V => Boolean, edgeFilterFunction: E => Boolean): LG =
    Subgraph.both[G, V, E, LG, GC](vertexFilterFunction, edgeFilterFunction).execute(this)

  def vertexInducedSubgraph(vertexFilterFunction: V => Boolean): LG =
    Subgraph.vertexInduced[G, V, E, LG, GC](vertexFilterFunction).execute(this)

  def edgeInducedSubgraph(edgeFilterFunction: E => Boolean): LG =
    Subgraph.edgeIncuded[G, V, E, LG, GC](edgeFilterFunction).execute(this)

}
