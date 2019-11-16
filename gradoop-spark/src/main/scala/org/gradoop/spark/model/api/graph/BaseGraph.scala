package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.Layout
import org.gradoop.spark.model.api.operators.BaseGraphOperators
import org.gradoop.spark.model.impl.types.GveGraphLayout

abstract class BaseGraph[L <: GveGraphLayout]
(layout: Layout[L], val config: GradoopSparkConfig[L]) extends ElementAccess[L#V, L#E] with BaseGraphOperators[L] {

  override def vertices: Dataset[L#V] = layout.vertices

  override def verticesByLabel(label: String): Dataset[L#V] = layout.verticesByLabel(label)

  override def edges: Dataset[L#E] = layout.edges

  override def edgesByLabel(label: String): Dataset[L#E] = layout.edgesByLabel(label)

  def factory: BaseGraphFactory[L]
}
