package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.operators.UnaryBaseGraphToValueOperator
import org.gradoop.spark.model.impl.types.LayoutType

trait LogicalGraphOperators[L <: LayoutType[L]] {
  this: L#LG =>

  def subgraph(vertexFilterExpression: String, edgeFilterExpression: String): L#LG

  def vertexInducedSubgraph(vertexFilterExpression: String): L#LG

  def edgeInducedSubgraph(edgeFilterExpression: String): L#LG

  def verify: L#LG

  def callForValue[V](operator: UnaryBaseGraphToValueOperator[L#LG, V]): V = operator.execute(this)
}
