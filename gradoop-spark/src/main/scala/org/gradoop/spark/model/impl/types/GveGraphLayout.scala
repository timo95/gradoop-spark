package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}

class GveGraphLayout extends GraphLayout {
  type G <: GraphHead//Ident with Labeled with Attributed
  type V <: Vertex//Ident with Labeled with Attributed with Member
  type E <: Edge//Ident with Labeled with Attributed with Member with Edge
}
