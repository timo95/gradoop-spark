package org.gradoop.spark.model.api.graph

import org.apache.spark.api.java.function.FilterFunction
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.impl.operators.subgraph.Subgraph

trait LogicalGraphOperators[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]] {
  this: LG =>

  def subgraph(vertexFilterFunction: FilterFunction[V], edgeFilterFunction: FilterFunction[E]): LG = {
    new Subgraph[G, V, E, LG, GC](vertexFilterFunction, edgeFilterFunction).execute(this)
  }

}
