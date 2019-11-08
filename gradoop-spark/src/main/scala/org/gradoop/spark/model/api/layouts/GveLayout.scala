package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.functions.filter.HasLabel

abstract class GveLayout[G <: GraphHead, V <: Vertex, E <: Edge](val graphHeads: Dataset[G], val vertices: Dataset[V], val edges: Dataset[E])
  extends GraphCollectionLayout[G, V, E] with LogicalGraphLayout[G, V, E] {

  override def graphHead: Dataset[G] = graphHeads
  override def graphHeadsByLabel(label: String): Dataset[G] = graphHeads.filter(new HasLabel[G](label))
  override def verticesByLabel(label: String): Dataset[V] = vertices.filter(new HasLabel[V](label))
  override def edgesByLabel(label: String): Dataset[E] = edges.filter(new HasLabel[E](label))
}
