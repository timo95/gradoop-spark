package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.GveLayout
import org.gradoop.spark.model.impl.types.GveLayoutType

/** Creates a logical graph with a specific layout. */
class LogicalGraphFactory[L <: GveLayoutType] extends BaseGraphFactory[L, LogicalGraph[L]] {

  override def createGraph(layout: GveLayout[L], config: GradoopSparkConfig[L]): LogicalGraph[L] = {
    new LogicalGraph[L](layout, config)
  }
}
