package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.elements.{EdgeType, GraphMember, Identifiable}

class GraphLayout {
  type L = this.type
  type M <: GraphModel
  type Ident = Identifiable
  type Edgy = EdgeType
  type Member = GraphMember
}
