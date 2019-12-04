package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.BaseGraph

trait UnaryBaseGraphToValueOperator[BG <: BaseGraph[_], V] extends Operator {
  def execute(graph: BG): V
}
