package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.gve.{GveEdge, GveGraphHead, GveVertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.gve.{GveBaseLayoutFactory, GveGraphCollectionOperators, GveLayout, GveLogicalGraphOperators}

class Gve[T <: Gve[T]] extends LayoutType[T] {
  type L <: GveLayout[T]

  type G <: GveGraphHead
  type V <: GveVertex
  type E <: GveEdge

  type LG <: LogicalGraph[T] with GveLogicalGraphOperators[T]
  type GC <: GraphCollection[T] with GveGraphCollectionOperators[T]

  type LGF <: GveBaseLayoutFactory[T, LG]
  type GCF <: GveBaseLayoutFactory[T, GC]

  type LabeledGraphHead = G
  type LabeledVertex = V
  type LabeledEdge = E
}
