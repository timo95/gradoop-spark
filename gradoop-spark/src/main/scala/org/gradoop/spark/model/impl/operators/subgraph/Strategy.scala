package org.gradoop.spark.model.impl.operators.subgraph

object Strategy extends Enumeration {
  type Strategy = Value
  val BOTH, VERTEX_INDUCED, EDGE_INDUCED = Value
  // TODO: Add EDGE_INDUCED_PROJECT_FIRST?
}
