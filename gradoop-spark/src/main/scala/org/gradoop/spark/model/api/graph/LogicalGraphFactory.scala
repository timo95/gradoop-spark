package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.LayoutType

/** Creates a logical graph with a specific layout. */
class LogicalGraphFactory[L <: LayoutType[L]] extends BaseGraphFactory[L, LogicalGraph[L]] {

  override def createGraph(layout: L#L, config: GradoopSparkConfig[L]): LogicalGraph[L] = {
    new LogicalGraph[L](layout, config)
  }
}
