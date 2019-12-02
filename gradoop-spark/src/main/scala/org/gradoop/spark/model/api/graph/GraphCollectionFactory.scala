package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.GveLayout
import org.gradoop.spark.model.impl.types.GveLayoutType

/** Creates a graph collection with a specific layout. */
class GraphCollectionFactory[L <: GveLayoutType[L]] extends BaseGraphFactory[L, GraphCollection[L]] {

  override def createGraph(layout: L#L, config: GradoopSparkConfig[L]): GraphCollection[L] = {
    new GraphCollection[L](layout, config)
  }
}
