package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayout, GraphCollectionLayoutFactory, GveBaseLayoutFactory, GveLayout}
import org.gradoop.spark.model.impl.types.GveLayoutType

class GraphCollection[L <: GveLayoutType](layout: GveLayout[L] with GraphCollectionLayout[L], config: GradoopSparkConfig[L])
  extends BaseGraph[L](layout, config) with GraphCollectionOperators[L] {

  override def factory: GveBaseLayoutFactory[L, GraphCollection[L]] with GraphCollectionLayoutFactory[L] = config.graphCollectionFactory
}
