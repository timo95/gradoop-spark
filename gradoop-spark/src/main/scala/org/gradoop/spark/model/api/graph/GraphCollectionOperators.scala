package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.impl.types.GveLayoutType

trait GraphCollectionOperators[L <: GveLayoutType] {
  this: GraphCollection[L] =>

}
