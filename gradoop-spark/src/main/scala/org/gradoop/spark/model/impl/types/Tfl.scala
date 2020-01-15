package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.tfl.{TflEdge, TflGraphHead, TflProperty, TflVertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.tfl.{TflBaseLayoutFactory, TflGraphCollectionOperators, TflLayout, TflLogicalGraphOperators}

class Tfl[T <: Tfl[T]] extends LayoutType[T] {
  type L <: TflLayout[T]

  type G <: TflGraphHead
  type V <: TflVertex
  type E <: TflEdge
  type P <: TflProperty

  type LG <: LogicalGraph[T] with TflLogicalGraphOperators[T]
  type GC <: GraphCollection[T] with TflGraphCollectionOperators[T]

  type LGF <: TflBaseLayoutFactory[T, LG]
  type GCF <: TflBaseLayoutFactory[T, GC]

  type LabeledGraphHead = G
  type LabeledVertex = V
  type LabeledEdge = E
}
