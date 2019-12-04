package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.BaseLayoutFactory
import org.gradoop.spark.model.impl.types.LayoutType

abstract class BaseGraph[L <: LayoutType[L]](val layout: L#L, val config: GradoopSparkConfig[L]) {

  def factory: BaseLayoutFactory[L]
}
