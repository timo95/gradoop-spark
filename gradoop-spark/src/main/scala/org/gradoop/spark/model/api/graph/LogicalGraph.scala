package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.LogicalGraphLayout
import org.gradoop.spark.model.impl.types.LayoutType

abstract class LogicalGraph[L <: LayoutType[L]](layout: L#L with LogicalGraphLayout[L], config: GradoopSparkConfig[L])
  extends BaseGraph[L](layout, config) with LogicalGraphOperators[L] {
  this: L#LG =>

  override def factory: L#LGF = config.logicalGraphFactory
}
