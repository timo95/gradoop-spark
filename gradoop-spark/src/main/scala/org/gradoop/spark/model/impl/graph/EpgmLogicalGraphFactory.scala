package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraphFactory
import org.gradoop.spark.model.api.layouts.LogicalGraphLayoutFactory

class EpgmLogicalGraphFactory(layoutFactory: LogicalGraphLayoutFactory[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends LogicalGraphFactory[G, V, E, LG, GC](layoutFactory, config) {

  override def init(graphHead: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): LG = {
    new EpgmLogicalGraph(layoutFactory(graphHead, vertices, edges), config)
  }
}
