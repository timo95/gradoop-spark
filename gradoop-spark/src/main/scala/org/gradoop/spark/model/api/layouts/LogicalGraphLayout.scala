package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}

trait LogicalGraphLayout[G <: GraphHead, V <: Vertex, E <: Edge] extends Layout[V, E] {

  /**
   * Returns a dataset containing a single graph head associated with that logical graph.
   *
   * @return 1-element dataset
   */
  def graphHead: Dataset[G]
}
