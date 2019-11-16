package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait GraphCollectionLayoutFactory[L <: GveGraphLayout] extends BaseLayoutFactory[L] {
  def apply(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): GraphCollectionLayout[L]

  def createGraphCollection(layout: GraphCollectionLayout[L], config: GradoopSparkConfig[L]): GraphCollection[L]
}
