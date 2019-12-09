package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.LayoutType

trait GraphCollectionOperators[L <: LayoutType[L]] {
  this: L#GC =>



  def callForValue[V](operator: UnaryGraphCollectionToValueOperator[L#GC, V]): V = {
    operator.execute(this)
  }
}
