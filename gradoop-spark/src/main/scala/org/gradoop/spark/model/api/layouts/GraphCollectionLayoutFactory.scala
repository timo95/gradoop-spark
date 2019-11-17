package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.impl.types.GveLayoutType

trait GraphCollectionLayoutFactory[L <: GveLayoutType] extends BaseLayoutFactory[L] {

  def apply(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): GveLayout[L] with GraphCollectionLayout[L]

  def createGraphCollection(layout: GveLayout[L] with GraphCollectionLayout[L], config: GradoopSparkConfig[L]): GraphCollection[L]
}
