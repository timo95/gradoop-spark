package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.operators.LogicalGraphOperators

abstract class LogicalGraph extends BaseGraph with LogicalGraphOperators {

  /**
   * Returns a Dataset containing a single graph head associated with that logical graph.
   *
   * @return 1-element Dataset
   */
  def getGraphHead: Dataset[G]
}
