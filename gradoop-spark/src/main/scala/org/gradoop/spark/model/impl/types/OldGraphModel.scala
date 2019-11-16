package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait OldGraphModel {
  type G <: GraphHead
  type V <: Vertex
  type E <: Edge
  //type LG <: LogicalGraph[L]
  //type GC <: GraphCollection[L]
}
