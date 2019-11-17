package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.functions.filter.HasLabel
import org.gradoop.spark.model.api.graph.ElementAccess
import org.gradoop.spark.model.impl.types.GveLayoutType

abstract class GveLayout[L <: GveLayoutType](val graphHeads: Dataset[L#G], val vertices: Dataset[L#V], val edges: Dataset[L#E])
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
  def graphHeadsByLabel(label: String): Dataset[L#G] = graphHeads.filter(new HasLabel[L#G](label))

  override def verticesByLabel(label: String): Dataset[L#V] = vertices.filter(new HasLabel[L#V](label))
  override def edgesByLabel(label: String): Dataset[L#E] = edges.filter(new HasLabel[L#E](label))
}
