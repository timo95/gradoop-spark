package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.LayoutType

class LogicalGraph[L <: LayoutType[L]](layout: L#L, config: GradoopSparkConfig[L])
  extends BaseGraph[L](layout, config) with LogicalGraphOperators[L] {

  override def factory: L#LGF = config.logicalGraphFactory
}
