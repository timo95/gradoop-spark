package org.gradoop.spark.model.impl.operators.subgraph.tfl

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.impl.operators.subgraph.Strategy.Strategy
import org.gradoop.spark.model.impl.operators.subgraph.{Strategy, Subgraph}
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflSubgraph[L <: Tfl[L]](vertexFilterExpression: Column, edgeFilterExpression: Column, strategy: Strategy)
  extends Subgraph[L] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    strategy match {
      case Strategy.BOTH =>
        /*
        val filterString = vertexFilterExpression.toString

        val filtersElements = filterString.contains(ColumnNames.ID) || filterString.contains(ColumnNames.LABEL)
        val filtersProperties = filterString.contains(ColumnNames.PROPERTIES)

        if(filtersElements && filtersProperties) {

        } else if(filtersProperties) {

        } else {

        }*/

        // Filter element + properties combined
        val vertices = graph.verticesWithProperties.mapValues(_.filter(vertexFilterExpression))
        val edges = graph.edgesWithProperties.mapValues(_.filter(edgeFilterExpression))

        // Split element and properties
        val (resVert, resVertProp) = TflFunctions.splitVertexMap(vertices)
        val (resEdge, resEdgeProp) = TflFunctions.splitEdgeMap(edges)
        graph.factory.init(graph.graphHead, resVert, resEdge, graph.graphHeadProperties, resVertProp, resEdgeProp)

      case Strategy.VERTEX_INDUCED =>
        graph // TODO
      case Strategy.EDGE_INDUCED =>
        graph
    }
  }
}

object TflSubgraph {

  def both[L <: Tfl[L]](vertexFilterExpression: Column, edgeFilterExpression: Column): TflSubgraph[L] = {
    new TflSubgraph(vertexFilterExpression, edgeFilterExpression, Strategy.BOTH)
  }

  def vertexInduced[L <: Tfl[L]](vertexFilterExpression: Column): TflSubgraph[L] = {
    new TflSubgraph(vertexFilterExpression, FilterExpressions.any, Strategy.VERTEX_INDUCED)
  }

  def edgeIncuded[L <: Tfl[L]](edgeFilterExpression: Column): TflSubgraph[L] = {
    new TflSubgraph(FilterExpressions.any, edgeFilterExpression, Strategy.EDGE_INDUCED)
  }
}
