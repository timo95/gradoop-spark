package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.elements.{EdgeType, GraphMember, Identifiable}
import org.gradoop.spark.model.api.layouts.Layout

class LayoutType {
  type L <: Layout[_]
  type M <: ModelType
  type Ident = Identifiable
  type Edgy = EdgeType
  type Member = GraphMember
}
