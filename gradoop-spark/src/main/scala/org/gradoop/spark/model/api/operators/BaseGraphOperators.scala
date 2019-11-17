package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.impl.types.GveLayoutType

trait BaseGraphOperators[L <: GveLayoutType] {
  this: BaseGraph[L] =>

}
