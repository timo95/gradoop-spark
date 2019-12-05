package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.LogicalGraph

trait UnaryLogicalGraphToLogicalGraphOperator[LG <: LogicalGraph[_]]
  extends UnaryLogicalGraphToValueOperator[LG, LG]
