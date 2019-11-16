package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait LogicalGraphLayout[L <: GveGraphLayout] extends Layout[L] {

  /**
   * Returns a dataset containing a single graph head associated with that logical graph.
   *
   * @return 1-element dataset
   */
  def graphHead: Dataset[L#G]
}
