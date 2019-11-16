package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayout, Layout}
import org.gradoop.spark.model.impl.types.GveGraphLayout

class GraphCollection[L <: GveGraphLayout]
(layout: GraphCollectionLayout[L], config: GradoopSparkConfig[L])
  extends BaseGraph[L](layout, config) with GraphCollectionOperators[L] {

  /**
   * Returns the graph heads associated with the logical graphs in that collection.
   *
   * @return graph heads
   */
  def graphHeads: Dataset[L#G] = layout.graphHeads

  /**
   * Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadsByLabel(label: String): Dataset[L#G] = layout.graphHeadsByLabel(label)

  override def factory: GraphCollectionFactory[L] = config.graphCollectionFactory
}
