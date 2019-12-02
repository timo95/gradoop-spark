package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.components.{EdgeType, GraphMember, Identifiable}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, Layout, LogicalGraphLayoutFactory}

class LayoutType[T <: LayoutType[T]] {
  type L <: Layout[T]
  type M <: ModelType
  type Ident = Identifiable
  type Edgy = EdgeType
  type Member = GraphMember

  type LGF <: LogicalGraphLayoutFactory[T]
  type GCF <: GraphCollectionLayoutFactory[T]
}
