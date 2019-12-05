package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.GraphCollection

trait UnaryGraphCollectionToValueOperator[GC <: GraphCollection[_], V] {
  def execute(graph: GC): V
}
