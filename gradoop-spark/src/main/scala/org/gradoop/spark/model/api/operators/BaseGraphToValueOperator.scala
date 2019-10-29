package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.BaseGraph

trait BaseGraphToValueOperator[BG <: BaseGraph[_, _, _, _, _], V] {
  def execute(graph: BG): V
}
