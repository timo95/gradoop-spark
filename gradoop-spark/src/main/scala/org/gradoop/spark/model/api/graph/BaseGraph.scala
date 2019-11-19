package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{GveBaseLayoutFactory, GveLayout}
import org.gradoop.spark.model.api.operators.BaseGraphOperators
import org.gradoop.spark.model.impl.types.GveLayoutType

abstract class BaseGraph[L <: GveLayoutType](val layout: GveLayout[L], val config: GradoopSparkConfig[L])
  extends BaseGraphOperators[L] {

  def factory: GveBaseLayoutFactory[L, _]
}
