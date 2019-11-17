package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.impl.types.GveLayoutType

trait LogicalGraphLayoutFactory[L <: GveLayoutType] extends BaseLayoutFactory[L] {
  def apply(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): GveLayout[L] with LogicalGraphLayout[L]

  def createLogicalGraph(layout: GveLayout[L] with LogicalGraphLayout[L], config: GradoopSparkConfig[L]): LogicalGraph[L]
}
