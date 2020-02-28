package org.gradoop.spark.model.impl.operators.subgraph.gve

import org.apache.spark.sql.Column
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.operators.subgraph.{Strategy, Subgraph}
import org.gradoop.spark.model.impl.types.Gve

class GveSubgraph[L <: Gve[L]](vertexFilterExpression: Column, edgeFilterExpression: Column, strategy: Strategy)
  extends Subgraph[L] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    import graph.config.sparkSession.implicits._

    strategy match {
      case Strategy.BOTH =>
        val filteredVertices = graph.vertices.filter(vertexFilterExpression)
        val filteredEdges = graph.edges.filter(edgeFilterExpression)
        graph.factory.init(graph.graphHead, filteredVertices, filteredEdges)

      case Strategy.VERTEX_INDUCED =>
        val filteredVertices = graph.vertices.filter(vertexFilterExpression)
        graph.factory.init(graph.graphHead, filteredVertices, graph.edges).removeDanglingEdges

      case Strategy.EDGE_INDUCED =>
        val filteredEdges = graph.edges.filter(edgeFilterExpression)
        val inducedVertices = graph.vertices
          .joinWith(filteredEdges, graph.vertices.id isin (filteredEdges.sourceId, filteredEdges.targetId))
          .select("_1.*").as[L#V]
          .dropDuplicates(ColumnNames.ID)
        graph.factory.init(graph.graphHead, inducedVertices, filteredEdges)
    }
  }
}

object GveSubgraph {

  def both[L <: Gve[L]](vertexFilterExpression: Column, edgeFilterExpression: Column): GveSubgraph[L] = {
    new GveSubgraph(vertexFilterExpression, edgeFilterExpression, Strategy.BOTH)
  }

  def vertexInduced[L <: Gve[L]](vertexFilterExpression: Column): GveSubgraph[L] = {
    new GveSubgraph(vertexFilterExpression, FilterExpressions.any, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: Gve[L]](edgeFilterExpression: Column): GveSubgraph[L] = {
    new GveSubgraph(FilterExpressions.any, edgeFilterExpression, Strategy.EDGE_INDUCED)
  }
}
