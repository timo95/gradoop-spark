package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait GraphModel {
  type G <: GraphHead
  type V <: Vertex
  type E <: Edge
  type LG <: LogicalGraph[G, V, E, LG, GC]
  type GC <: GraphCollection[G, V, E, LG, GC]
}
