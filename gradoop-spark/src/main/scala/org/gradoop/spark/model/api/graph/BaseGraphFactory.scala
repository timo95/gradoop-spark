package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.GveLayout
import org.gradoop.spark.model.impl.types.GveLayoutType

trait BaseGraphFactory[L <: GveLayoutType, G <: BaseGraph[L]] extends Serializable {

  def createGraph(layout: GveLayout[L], config: GradoopSparkConfig[L]): G
}
