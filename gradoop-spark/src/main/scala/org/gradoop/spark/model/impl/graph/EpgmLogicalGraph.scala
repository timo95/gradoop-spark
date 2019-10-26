package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.api.layouts.LogicalGraphLayout

class EpgmLogicalGraph(layout: LogicalGraphLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC]) extends LogicalGraph[G, V, E, LG, GC](layout, config) {

  override def getGraphHead: Dataset[G] = layout.getGraphHead
  override def getVertices: Dataset[V] = layout.getVertices
  override def getVerticesByLabel(label: String): Dataset[V] = layout.getVerticesByLabel(label)
  override def getEdges: Dataset[E] = layout.getEdges
  override def getEdgesByLabel(label: String): Dataset[E] = layout.getEdgesByLabel(label)
}
