package org.gradoop.spark.model.api

import org.gradoop.common.model.api.entities.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait GraphModel {
  type G <: GraphHead
  type V <: Vertex
  type E <: Edge
  type LG <: LogicalGraph
  type GC <: GraphCollection
}
