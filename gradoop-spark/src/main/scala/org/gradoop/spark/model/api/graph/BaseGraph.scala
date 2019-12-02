package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{BaseLayoutFactory, GveBaseLayoutFactory, GveLayout, Layout}
import org.gradoop.spark.model.api.operators.BaseGraphOperators
import org.gradoop.spark.model.impl.types.{GveLayoutType, LayoutType}

abstract class BaseGraph[L <: LayoutType[L]](val layout: L#L, val config: GradoopSparkConfig[L])
  extends BaseGraphOperators[L] {

  def factory: BaseLayoutFactory[L]
}
