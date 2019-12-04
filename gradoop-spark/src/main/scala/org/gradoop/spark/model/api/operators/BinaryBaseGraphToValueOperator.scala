package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.BaseGraph

trait BinaryBaseGraphToValueOperator[BG <: BaseGraph[_], V] extends Operator {
  def execute(left: BG, right: BG): V
}
