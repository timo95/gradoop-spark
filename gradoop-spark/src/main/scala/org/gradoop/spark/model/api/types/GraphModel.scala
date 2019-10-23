package org.gradoop.spark.model.api.types

import org.gradoop.spark.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait GraphModel {
  type G <: GraphHead
  type V <: Vertex
  type E <: Edge
  type LG <: LogicalGraph
  type GC <: GraphCollection
}
