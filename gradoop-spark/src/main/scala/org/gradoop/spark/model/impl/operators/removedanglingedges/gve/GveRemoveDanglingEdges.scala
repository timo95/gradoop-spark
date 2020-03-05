package org.gradoop.spark.model.impl.operators.removedanglingedges.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveRemoveDanglingEdges[L <: Gve[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    import graph.config.sparkSession.implicits._

    val vertexIds = graph.vertices.select(ColumnNames.ID).cache

    val filteredEdgesSource = graph.edges
      .joinWith(vertexIds, graph.edges.sourceId === vertexIds(ColumnNames.ID))
      .select("_1.*").as[L#E]
    val filteredEdges = filteredEdgesSource
      .joinWith(vertexIds, filteredEdgesSource.targetId === vertexIds(ColumnNames.ID))
      .select("_1.*").as[L#E]
    graph.factory.init(graph.graphHead, graph.vertices, filteredEdges)
  }
}
