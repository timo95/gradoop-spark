package org.gradoop.spark.model.api.graph

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait GraphCollectionOperators[L <: GveGraphLayout] {
  this: GraphCollection[L] =>

}
