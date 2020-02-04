package org.gradoop.spark.model.impl.operators.subgraph.tfl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.gradoop.common.util.ColumnNames
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
    implicit val sparkSession = graph.config.sparkSession
    import sparkSession.implicits._

    strategy match {
      case Strategy.BOTH =>
        // Filter element + properties combined
        val filteredVertices = graph.verticesWithProperties
          .mapValues(_.filter(vertexFilterExpression))
        val filteredEdges = graph.edgesWithProperties
          .mapValues(_.filter(edgeFilterExpression))

        // Split element and properties
        val (resVert, resVertProp) = TflFunctions.splitVertexMap(filteredVertices)
        val (resEdge, resEdgeProp) = TflFunctions.splitEdgeMap(filteredEdges)
        graph.factory.init(graph.graphHead, resVert, resEdge,
          graph.graphHeadProperties, resVertProp, resEdgeProp)

      case Strategy.VERTEX_INDUCED =>
        // Filter element + properties combined
        val filteredVertices = graph.verticesWithProperties
          .mapValues(_.filter(vertexFilterExpression))

        // Split element and properties
        val (resVert, resVertProp) = TflFunctions.splitVertexMap(filteredVertices)
        graph.factory.init(graph.graphHead, resVert, graph.edges,
          graph.graphHeadProperties, resVertProp, graph.edgeProperties).verify

      case Strategy.EDGE_INDUCED =>
        // Filter element + properties combined
        val filteredEdges = graph.edgesWithProperties
          .mapValues(_.filter(edgeFilterExpression))

        // Induce vertices from edges
        val unionEdges = TflFunctions.reduceUnion(filteredEdges.values) // single dataset
        val inducedVertices = graph.vertices.mapValues(v =>
          v.joinWith(unionEdges, v.id isin(col(ColumnNames.SOURCE_ID), col(ColumnNames.TARGET_ID)))
            .map(t => t._1)
            .dropDuplicates(ColumnNames.ID))

        // Induce properties from elements
        val inducedVertProp = TflFunctions.inducePropMap(inducedVertices,
          graph.vertexProperties)

        // Split element and properties
        val (resEdge, resEdgeProp) = TflFunctions.splitEdgeMap(filteredEdges)
        graph.factory.init(graph.graphHead, inducedVertices, resEdge,
          graph.graphHeadProperties, inducedVertProp, resEdgeProp)
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
