package org.gradoop.spark.model.api.layouts

import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionOperators}
import org.gradoop.spark.model.impl.types.GveLayoutType

trait GveGraphCollectionOperators[L <: GveLayoutType[L]] extends GraphCollectionOperators[L] {
  this: GraphCollection[L] =>

}
