package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.Layout

abstract class GraphCollection[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(layout: Layout[V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends BaseGraph[G, V, E, LG, GC](layout, config) with GraphCollectionOperators[G, V, E, LG, GC] {
  this: GC =>

  /**
   * Returns the graph heads associated with the logical graphs in that collection.
   *
   * @return graph heads
   */
  def graphHeads: Dataset[G]

  /**
   * Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadsByLabel(label: String): Dataset[G]

  override def factory: GraphCollectionFactory[G, V, E, LG, GC] = config.graphCollectionFactory
}
