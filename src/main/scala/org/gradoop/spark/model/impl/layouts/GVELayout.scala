package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.impl.pojo.{EPGMEdge, EPGMGraphHead, EPGMVertex}
import org.gradoop.spark.model.api.EPGM
import org.gradoop.spark.model.api.layouts.GraphCollectionLayout

class GVELayout(graphHeads: Dataset[EPGMGraphHead], vertices: Dataset[EPGMVertex], edges: Dataset[EPGMEdge])
  extends GraphCollectionLayout with EPGM {

  override def getGraphHeads: Dataset[EPGMGraphHead] = graphHeads

  override def getGraphHeadsByLabel(label: String): Dataset[EPGMGraphHead] = {
    graphHeads.filter(graphHead => graphHead.getLabel.equals(label))
  }

  override def getVertices: Dataset[EPGMVertex] = vertices

  override def getVerticesByLabel(label: String): Dataset[EPGMVertex] = {
    vertices.filter(vertices => vertices.getLabel.equals(label))
  }

  override def getEdges: Dataset[EPGMEdge] = edges

  override def getEdgesByLabel(label: String): Dataset[EPGMEdge] = {
    edges.filter(edges => edges.getLabel.equals(label))
  }
}
