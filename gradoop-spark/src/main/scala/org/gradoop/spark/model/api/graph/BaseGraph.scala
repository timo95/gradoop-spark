package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.Layout
import org.gradoop.spark.model.api.operators.BaseGraphOperators

abstract class BaseGraph[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(layout: Layout[V, E], val config: GradoopSparkConfig[G, V, E, LG, GC])
  extends ElementAccess[V, E] with BaseGraphOperators[G, V, E, LG, GC] {

  override def vertices: Dataset[V] = layout.vertices

  override def verticesByLabel(label: String): Dataset[V] = layout.verticesByLabel(label)

  override def edges: Dataset[E] = layout.edges

  override def edgesByLabel(label: String): Dataset[E] = layout.edgesByLabel(label)

  def factory: BaseGraphFactory[G, V, E, LG, GC]
}
