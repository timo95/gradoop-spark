package org.gradoop.spark.model.api.layouts.tfl

import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.types.Tfl

trait TflGraphCollectionOperators[L <: Tfl[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

}
