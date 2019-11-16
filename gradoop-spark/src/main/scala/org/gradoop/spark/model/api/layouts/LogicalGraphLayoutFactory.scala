package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait LogicalGraphLayoutFactory[L <: GveGraphLayout] extends BaseLayoutFactory[L] {
  def apply(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): LogicalGraphLayout[L]

  def createLogicalGraph(layout: LogicalGraphLayout[L], config: GradoopSparkConfig[L]): LogicalGraph[L]
}
