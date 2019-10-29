package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.LogicalGraph

trait LogicalGraphToLogicalGraphOperator[LG <: LogicalGraph[_, _, _, _, _]] extends BaseGraphToValueOperator[LG, LG] {

}
