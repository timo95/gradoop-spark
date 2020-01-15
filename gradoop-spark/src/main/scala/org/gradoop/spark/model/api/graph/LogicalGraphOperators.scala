package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Column
import org.gradoop.spark.model.api.operators._
import org.gradoop.spark.model.impl.types.LayoutType

trait LogicalGraphOperators[L <: LayoutType[L]] {
  this: L#LG =>

  def equalsByElementIds(other: L#LG): Boolean

  def equalsByElementData(other: L#LG): Boolean

  def equalsByData(other: L#LG): Boolean

  def subgraph(vertexFilterExpression: Column, edgeFilterExpression: Column): L#LG

  def vertexInducedSubgraph(vertexFilterExpression: Column): L#LG

  def edgeInducedSubgraph(edgeFilterExpression: Column): L#LG

  def verify: L#LG

  // Call for operators

  def callForValue[V](operator: UnaryLogicalGraphToValueOperator[L#LG, V]): V = {
    operator.execute(this)
  }

  def callForValue[V](operator: BinaryLogicalGraphToValueOperator[L#LG, V], other: L#LG): V = {
    operator.execute(this, other)
  }

  def callForGraph(operator: UnaryLogicalGraphToLogicalGraphOperator[L#LG]): L#LG = {
    callForValue(operator)
  }

  def callForGraph(operator: BinaryLogicalGraphToLogicalGraphOperator[L#LG], other: L#LG): L#LG = {
    callForValue(operator, other)
  }
}
