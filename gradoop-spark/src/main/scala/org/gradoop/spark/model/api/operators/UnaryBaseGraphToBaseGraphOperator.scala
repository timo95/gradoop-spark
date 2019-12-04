package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.BaseGraph

trait UnaryBaseGraphToBaseGraphOperator[BG <: BaseGraph[_]] extends UnaryBaseGraphToValueOperator[BG, BG] {

}
