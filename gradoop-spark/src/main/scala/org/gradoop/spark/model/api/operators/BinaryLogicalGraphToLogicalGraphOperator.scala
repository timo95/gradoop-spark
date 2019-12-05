package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.LogicalGraph

trait BinaryLogicalGraphToLogicalGraphOperator[LG <: LogicalGraph[_]]
  extends BinaryLogicalGraphToValueOperator[LG, LG]
