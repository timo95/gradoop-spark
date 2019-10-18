package org.gradoop.spark.model.impl.epgm.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.impl.epgm.Epgm
import org.gradoop.spark.model.impl.epgm.layouts.EpgmGraphLayout

class EpgmLogicalGraph(layout: EpgmGraphLayout) extends LogicalGraph with Epgm {
  var graphLayout: EpgmGraphLayout = layout

  override def getGraphHead: Dataset[G] = graphLayout.getGraphHead
  override def getVertices: Dataset[V] = graphLayout.getVertices
  override def getVerticesByLabel(label: String): Dataset[V] = graphLayout.getVerticesByLabel(label)
  override def getEdges: Dataset[E] = graphLayout.getEdges
  override def getEdgesByLabel(label: String): Dataset[E] = graphLayout.getEdgesByLabel(label)
}
