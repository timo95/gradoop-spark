package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.GraphModel

trait GraphCollectionLayout extends Layout {

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
