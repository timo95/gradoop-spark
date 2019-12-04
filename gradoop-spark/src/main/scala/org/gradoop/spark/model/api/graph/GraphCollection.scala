package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.GraphCollectionLayout
import org.gradoop.spark.model.impl.types.LayoutType

abstract class GraphCollection[L <: LayoutType[L]](layout: L#L with GraphCollectionLayout[L], config: GradoopSparkConfig[L])
  extends BaseGraph[L](layout, config) with GraphCollectionOperators[L] {
  this: L#GC =>

  override def factory: L#GCF = config.graphCollectionFactory
}
