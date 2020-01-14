package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.GraphCollection

trait BinaryGraphCollectionToValueOperator[GC <: GraphCollection[_], V] extends Operator {
  def execute(left: GC, right: GC): V
}
