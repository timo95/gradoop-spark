package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.impl.types.LayoutType

trait GraphCollectionOperators[L <: LayoutType[L]] {
  this: GraphCollection[L] =>

}
