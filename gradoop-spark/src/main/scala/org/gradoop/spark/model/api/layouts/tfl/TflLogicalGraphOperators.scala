package org.gradoop.spark.model.api.layouts.tfl

import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.types.Tfl

trait TflLogicalGraphOperators[L <: Tfl[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

}
