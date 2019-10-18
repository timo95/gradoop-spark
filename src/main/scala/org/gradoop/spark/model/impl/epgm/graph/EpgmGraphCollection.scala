package org.gradoop.spark.model.impl.epgm.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.impl.epgm.Epgm
import org.gradoop.spark.model.impl.epgm.layouts.EpgmGraphCollectionLayout

class EpgmGraphCollection(layout: EpgmGraphCollectionLayout) extends GraphCollection with Epgm {
  var graphLayout: EpgmGraphCollectionLayout = layout
  //var graphLayout:GraphCollectionLayout

  override def getGraphHeads: Dataset[G] = graphLayout.getGraphHeads
  override def getGraphHeadsByLabel(label: String): Dataset[G] = graphLayout.getGraphHeadsByLabel(label)
  override def getVertices: Dataset[V] = graphLayout.getVertices
  override def getVerticesByLabel(label: String): Dataset[V] = graphLayout.getVerticesByLabel(label)
  override def getEdges: Dataset[E] = graphLayout.getEdges
  override def getEdgesByLabel(label: String): Dataset[E] = graphLayout.getEdgesByLabel(label)
}
