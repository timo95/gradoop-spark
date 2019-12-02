package org.gradoop.spark.model.api.graph

import org.gradoop.common.model.api.gve.{Edge, Vertex}
import org.gradoop.spark.model.impl.operators.subgraph.Subgraph
import org.gradoop.spark.model.impl.types.LayoutType

trait LogicalGraphOperators[L <: LayoutType[L]] {
  this: LogicalGraph[L] =>

  // Layout agnostic

  def subgraph(vertexFilterExpression: String, edgeFilterExpression: String): LogicalGraph[L]

  def vertexInducedSubgraph(vertexFilterExpression: String): LogicalGraph[L]

  def edgeInducedSubgraph(edgeFilterExpression: String): LogicalGraph[L]

  def verify: LogicalGraph[L]

  // Specific elements

  def subgraph(vertexFilterFunction: Vertex => Boolean, edgeFilterFunction: Edge => Boolean): LogicalGraph[L] =
    Subgraph.both[L](vertexFilterFunction, edgeFilterFunction).execute(this)

  def vertexInducedSubgraph(vertexFilterFunction: Vertex => Boolean): LogicalGraph[L] =
    Subgraph.vertexInduced[L](vertexFilterFunction).execute(this)

  def edgeInducedSubgraph(edgeFilterFunction: Edge => Boolean): LogicalGraph[L] =
    Subgraph.edgeIncuded[L](edgeFilterFunction).execute(this)
}
