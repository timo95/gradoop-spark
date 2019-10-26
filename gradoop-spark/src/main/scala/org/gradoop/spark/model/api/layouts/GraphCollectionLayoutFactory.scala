package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, ElementFactoryProvider, GraphHead, Vertex}

trait GraphCollectionLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge] extends ElementFactoryProvider[G, V, E] {
  def apply(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GraphCollectionLayout[G, V, E]
}
