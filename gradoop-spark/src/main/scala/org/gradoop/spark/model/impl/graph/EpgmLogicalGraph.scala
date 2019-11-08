package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.api.layouts.LogicalGraphLayout

class EpgmLogicalGraph(layout: LogicalGraphLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends LogicalGraph[G, V, E, LG, GC](layout, config) {

  override def graphHead: Dataset[G] = layout.graphHead
  override def vertices: Dataset[V] = layout.vertices
  override def verticesByLabel(label: String): Dataset[V] = layout.verticesByLabel(label)
  override def edges: Dataset[E] = layout.edges
  override def edgesByLabel(label: String): Dataset[E] = layout.edgesByLabel(label)
}
