package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.api.layouts.GraphCollectionLayout

class EpgmGraphCollection(layout: GraphCollectionLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends GraphCollection[G, V, E, LG, GC](layout, config) {

  override def graphHeads: Dataset[G] = layout.graphHeads
  override def graphHeadsByLabel(label: String): Dataset[G] = layout.graphHeadsByLabel(label)
  override def vertices: Dataset[V] = layout.vertices
  override def verticesByLabel(label: String): Dataset[V] = layout.verticesByLabel(label)
  override def edges: Dataset[E] = layout.edges
  override def edgesByLabel(label: String): Dataset[E] = layout.edgesByLabel(label)
}
