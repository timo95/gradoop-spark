package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.LogicalGraphLayout

abstract class LogicalGraph[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(layout: LogicalGraphLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends BaseGraph[G, V, E, LG, GC](layout, config) with LogicalGraphOperators[G, V, E, LG, GC] {
  this: LG =>

  /**
   * Returns a Dataset containing a single graph head associated with that logical graph.
   *
   * @return 1-element Dataset
   */
  def graphHead: Dataset[G]

  override def factory: LogicalGraphFactory[G, V, E, LG, GC] = config.logicalGraphFactory
}
