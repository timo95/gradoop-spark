package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.api.layouts.GraphCollectionLayout

class EpgmGraphCollection(layout: GraphCollectionLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends GraphCollection[G, V, E, LG, GC](layout, config) {

  override def getGraphHeads: Dataset[G] = layout.getGraphHeads
  override def getGraphHeadsByLabel(label: String): Dataset[G] = layout.getGraphHeadsByLabel(label)
  override def getVertices: Dataset[V] = layout.getVertices
  override def getVerticesByLabel(label: String): Dataset[V] = layout.getVerticesByLabel(label)
  override def getEdges: Dataset[E] = layout.getEdges
  override def getEdgesByLabel(label: String): Dataset[E] = layout.getEdgesByLabel(label)
}
