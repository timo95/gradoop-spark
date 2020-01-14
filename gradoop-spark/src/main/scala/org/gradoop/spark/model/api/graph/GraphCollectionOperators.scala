package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.operators.{BinaryGraphCollectionToValueOperator, UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.LayoutType

trait GraphCollectionOperators[L <: LayoutType[L]] {
  this: L#GC =>

  def equalsByGraphIds(other: L#GC): Boolean

  def equalsByGraphElementIds(other: L#GC): Boolean

  def equalsByGraphElementData(other: L#GC): Boolean

  def equalsByGraphData(other: L#GC): Boolean

  // Call for operators

  def callForValue[V](operator: UnaryGraphCollectionToValueOperator[L#GC, V]): V = {
    operator.execute(this)
  }

  def callForValue[V](operator: BinaryGraphCollectionToValueOperator[L#GC, V], other: L#GC): V = {
    operator.execute(this, other)
  }
}
