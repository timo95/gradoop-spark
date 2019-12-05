package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.LogicalGraph

trait BinaryLogicalGraphToValueOperator[LG <: LogicalGraph[_], V] extends Operator {
  def execute(left: LG, right: LG): V
}
