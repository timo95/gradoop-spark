package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait GraphCollectionLayout[L <: GveGraphLayout] extends Layout[L] {

  /**
   * Returns the graph heads associated with the logical graphs in that collection.
   *
   * @return graph heads
   */
  def graphHeads: Dataset[L#G]

  /**
   * Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadsByLabel(label: String): Dataset[L#G]

}
