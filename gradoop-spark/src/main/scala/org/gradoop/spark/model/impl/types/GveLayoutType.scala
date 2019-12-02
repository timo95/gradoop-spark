package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.gve.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.{GveBaseLayoutFactory, GveLayout}

class GveLayoutType[T <: GveLayoutType[T]] extends LayoutType[T] {
  type L <: GveLayout[T]
  type G <: GraphHead
  type V <: Vertex
  type E <: Edge

  type LGF <: GveBaseLayoutFactory[T, LogicalGraph[T]]
  type GCF <: GveBaseLayoutFactory[T, GraphCollection[T]]
}
