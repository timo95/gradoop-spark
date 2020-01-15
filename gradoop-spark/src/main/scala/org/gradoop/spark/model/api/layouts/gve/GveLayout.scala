package org.gradoop.spark.model.api.layouts.gve

import org.apache.spark.sql.Dataset
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayout, LogicalGraphLayout}
import org.gradoop.spark.model.impl.types.Gve

abstract class GveLayout[L <: Gve[L]](val graphHeads: Dataset[L#G], val vertices: Dataset[L#V], val edges: Dataset[L#E])
  extends GraphCollectionLayout[L] with LogicalGraphLayout[L] {

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
  def graphHeadsByLabel(label: String): Dataset[L#G] = graphHeads.filter(FilterExpressions.hasLabel(label))

  /** Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def verticesByLabel(label: String): Dataset[L#V] = vertices.filter(FilterExpressions.hasLabel(label))

  /** Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def edgesByLabel(label: String): Dataset[L#E] = edges.filter(FilterExpressions.hasLabel(label))
}
