package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.LayoutType

trait BaseGraphFactory[L <: LayoutType[L], G <: BaseGraph[L]] extends Serializable {

  def createGraph(layout: L#L, config: GradoopSparkConfig[L]): G
}
