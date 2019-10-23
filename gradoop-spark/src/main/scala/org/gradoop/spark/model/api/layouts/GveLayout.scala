package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.elements.{Edge, GraphHead, Vertex}

abstract class GveLayout[G <: GraphHead, V <: Vertex, E <: Edge](graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E])
  extends GraphCollectionLayout[G, V, E] with LogicalGraphLayout[G, V, E] {

  override def getGraphHead: Dataset[G] = graphHeads

  override def getGraphHeads: Dataset[G] = graphHeads

  override def getGraphHeadsByLabel(label: String): Dataset[G] = {
    graphHeads.filter(graphHead => graphHead.getLabels.contains(label))
  }

  override def getVertices: Dataset[V] = vertices

  override def getVerticesByLabel(label: String): Dataset[V] = {
    vertices.filter(vertices => vertices.getLabels.contains(label))
  }

  override def getEdges: Dataset[E] = edges

  override def getEdgesByLabel(label: String): Dataset[E] = {
    edges.filter(edges => edges.getLabels.contains(label))
  }
}
