package org.gradoop.spark.model.api.layouts.gve

import org.apache.spark.sql.Dataset
import org.gradoop.spark.expressions.filter.FilterStrings
import org.gradoop.spark.model.api.layouts.{ElementAccess, GraphCollectionLayout, LogicalGraphLayout}
import org.gradoop.spark.model.impl.types.Gve

abstract class GveLayout[L <: Gve[L]](val graphHeads: Dataset[L#G], val vertices: Dataset[L#V], val edges: Dataset[L#E])
  extends GraphCollectionLayout[L] with LogicalGraphLayout[L] with ElementAccess[L#V, L#E] {

  /**
   * Returns a dataset containing a single graph head associated with that logical graph.
   *
   * @return 1-element dataset
   */
  def graphHead: Dataset[L#G] = graphHeads

  /**
   * Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadsByLabel(label: String): Dataset[L#G] = graphHeads.filter(FilterStrings.hasLabel(label))
}
