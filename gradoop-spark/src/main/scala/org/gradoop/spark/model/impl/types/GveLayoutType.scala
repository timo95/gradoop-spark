package org.gradoop.spark.model.impl.types

import org.gradoop.common.model.api.gve.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.layouts.GveLayout

class GveLayoutType extends LayoutType {
  type L <: GveLayout[_]
  type G <: GraphHead
  type V <: Vertex
  type E <: Edge
}
