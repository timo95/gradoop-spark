package org.gradoop.spark.model.impl.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollectionFactory
import org.gradoop.spark.model.api.layouts.GraphCollectionLayoutFactory

class EpgmGraphCollectionFactory(layoutFactory: GraphCollectionLayoutFactory[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends GraphCollectionFactory[G, V, E, LG, GC](layoutFactory, config) {

  override def init(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GC = {
    new EpgmGraphCollection(layoutFactory(graphHeads, vertices, edges), config)
  }
}
