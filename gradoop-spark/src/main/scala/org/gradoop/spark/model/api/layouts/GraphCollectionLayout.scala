package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.elements.{GraphHead, Vertex}

trait GraphCollectionLayout[G <: GraphHead, V <: Vertex, E <: Edge] extends Layout[V, E] {

  /**
   * Returns the graph heads associated with the logical graphs in that collection.
   *
   * @return graph heads
   */
  def getGraphHeads: Dataset[G]

  /**
   * Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def getGraphHeadsByLabel(label: String): Dataset[G]

}
