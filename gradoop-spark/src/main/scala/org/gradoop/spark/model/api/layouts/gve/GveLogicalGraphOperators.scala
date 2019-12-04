package org.gradoop.spark.model.api.layouts.gve

import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.operators.subgraph.gve.GveSubgraph
import org.gradoop.spark.model.impl.operators.verify.GveVerify
import org.gradoop.spark.model.impl.types.Gve

trait GveLogicalGraphOperators[L <: Gve[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

  def subgraph(vertexFilterExpression: String, edgeFilterExpression: String): L#LG =
    GveSubgraph.both[L](vertexFilterExpression, edgeFilterExpression).execute(this)

  def vertexInducedSubgraph(vertexFilterExpression: String): L#LG =
    GveSubgraph.vertexInduced[L](vertexFilterExpression).execute(this)

  def edgeInducedSubgraph(edgeFilterExpression: String): L#LG =
    GveSubgraph.edgeIncuded[L](edgeFilterExpression).execute(this)

  def verify: L#LG = new GveVerify[L]().execute(this)
}
