package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset

trait GraphLayout extends GraphCollectionLayout {

  /**
   * Returns a dataset containing a single graph head associated with that logical graph.
   *
   * @return 1-element dataset
   */
  def getGraphHead: Dataset[G]
}
