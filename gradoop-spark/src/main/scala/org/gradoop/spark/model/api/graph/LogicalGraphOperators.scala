package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.operators.{BinaryLogicalGraphToLogicalGraphOperator,
  BinaryLogicalGraphToValueOperator, UnaryLogicalGraphToLogicalGraphOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.LayoutType

trait LogicalGraphOperators[L <: LayoutType[L]] {
  this: L#LG =>

  def subgraph(vertexFilterExpression: String, edgeFilterExpression: String): L#LG

  def vertexInducedSubgraph(vertexFilterExpression: String): L#LG

  def edgeInducedSubgraph(edgeFilterExpression: String): L#LG

  def verify: L#LG

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
