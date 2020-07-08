package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, Layout, LogicalGraphLayoutFactory}

class LayoutType[T <: LayoutType[T]] {
  type L <: Layout[T]
  type M <: ModelType

  type LG <: LogicalGraph[T]
  type GC <: GraphCollection[T]

  type LGF <: LogicalGraphLayoutFactory[T]
  type GCF <: GraphCollectionLayoutFactory[T]
}
