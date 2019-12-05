package org.gradoop.spark.model.api.layouts.gve

import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.operators.subgraph.gve.GveSubgraph
import org.gradoop.spark.model.impl.operators.verify.GveVerify
import org.gradoop.spark.model.impl.types.Gve

trait GveLogicalGraphOperators[L <: Gve[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

  def subgraph(vertexFilterExpression: String, edgeFilterExpression: String): L#LG =
    callForGraph(GveSubgraph.both[L](vertexFilterExpression, edgeFilterExpression))

  def vertexInducedSubgraph(vertexFilterExpression: String): L#LG =
    callForGraph(GveSubgraph.vertexInduced[L](vertexFilterExpression))

  def edgeInducedSubgraph(edgeFilterExpression: String): L#LG =
    callForGraph(GveSubgraph.edgeIncuded[L](edgeFilterExpression))

  def verify: L#LG = callForGraph(new GveVerify[L])
}
