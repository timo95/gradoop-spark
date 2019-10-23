package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.api.layouts.LogicalGraphLayout
import org.gradoop.spark.util.EpgmGradoopSparkConfig

class EpgmLogicalGraph extends LogicalGraph {
  var layout: LogicalGraphLayout[G, V, E] = _
  var config: GradoopSparkConfig[G, V, E, LG, GC] = new EpgmGradoopSparkConfig()

  override def getGraphHead: Dataset[G] = layout.getGraphHead
  override def getVertices: Dataset[V] = layout.getVertices
  override def getVerticesByLabel(label: String): Dataset[V] = layout.getVerticesByLabel(label)
  override def getEdges: Dataset[E] = layout.getEdges
  override def getEdgesByLabel(label: String): Dataset[E] = layout.getEdgesByLabel(label)
}
