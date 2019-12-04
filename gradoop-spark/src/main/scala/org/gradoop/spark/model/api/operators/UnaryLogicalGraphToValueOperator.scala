package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.LogicalGraph

trait UnaryLogicalGraphToValueOperator[LG <: LogicalGraph[_], V] extends UnaryBaseGraphToValueOperator[LG, V] {

}
