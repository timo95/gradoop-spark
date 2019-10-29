package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.Layout
import org.gradoop.spark.model.api.operators.BaseGraphOperators

abstract class BaseGraph[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(layout: Layout[V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends ElementAccess[V, E] with BaseGraphOperators[G, V, E, LG, GC] {

  override def getVertices: Dataset[V] = layout.getVertices

  override def getVerticesByLabel(label: String): Dataset[V] = layout.getVerticesByLabel(label)

  override def getEdges: Dataset[E] = layout.getEdges

  override def getEdgesByLabel(label: String): Dataset[E] = layout.getEdgesByLabel(label)

  def getConfig: GradoopSparkConfig[G, V, E, LG, GC] = config

  def getFactory: BaseGraphFactory[G, V, E, LG, GC]
}
