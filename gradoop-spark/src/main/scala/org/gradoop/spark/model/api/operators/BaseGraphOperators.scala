package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.impl.types.LayoutType

trait BaseGraphOperators[L <: LayoutType[L]] {
  this: BaseGraph[L] =>

}
