package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}

trait LogicalGraphLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge] extends BaseLayoutFactory[G, V, E] {
  def apply(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): LogicalGraphLayout[G, V, E]
}
