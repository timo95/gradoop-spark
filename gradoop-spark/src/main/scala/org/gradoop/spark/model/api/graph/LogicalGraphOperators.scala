package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.impl.operators.subgraph.{Subgraph, SubgraphSql}
import org.gradoop.spark.model.impl.operators.verify.Verify
import org.gradoop.spark.model.impl.types.{GveLayoutType, LayoutType}

trait LogicalGraphOperators[L <: LayoutType[L]] {
  this: LogicalGraph[L] =>
/*
  def subgraph(vertexFilterFunction: L#V => Boolean, edgeFilterFunction: L#E => Boolean): LogicalGraph[L] =
    Subgraph.both[L](vertexFilterFunction, edgeFilterFunction).execute(this)

  def vertexInducedSubgraph(vertexFilterFunction: L#V => Boolean): LogicalGraph[L] =
    Subgraph.vertexInduced[L](vertexFilterFunction).execute(this)

  def edgeInducedSubgraph(edgeFilterFunction: L#E => Boolean): LogicalGraph[L] =
    Subgraph.edgeIncuded[L](edgeFilterFunction).execute(this)

  def subgraph(vertexFilterExpression: String, edgeFilterExpression: String): LogicalGraph[L] =
    SubgraphSql.both[L](vertexFilterExpression, edgeFilterExpression).execute(this)

  def vertexInducedSubgraph(vertexFilterExpression: String): LogicalGraph[L] =
    SubgraphSql.vertexInduced[L](vertexFilterExpression).execute(this)

  def edgeInducedSubgraph(edgeFilterExpression: String): LogicalGraph[L] =
    SubgraphSql.edgeIncuded[L](edgeFilterExpression).execute(this)*/

  //def verify(implicit ev: L <:< GveLayoutType[L]): LogicalGraph[L] = new Verify[L]().execute(this)

  /* Structural Type bound
  def verify(implicit ev: L <:< {def verify: LogicalGraph[L]}): Unit = {
    layout.verify
  }*/
}
