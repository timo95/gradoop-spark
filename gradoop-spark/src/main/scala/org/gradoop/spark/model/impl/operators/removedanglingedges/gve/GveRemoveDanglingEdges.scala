package org.gradoop.spark.model.impl.operators.removedanglingedges.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveRemoveDanglingEdges[L <: Gve[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    import graph.config.Implicits._
    val factory = graph.factory
    import factory.Implicits._

    val vertices = graph.vertices.cache

    val filteredEdgesSource = graph.edges
      .join(vertices, graph.edges.sourceId === vertices(ColumnNames.ID), "leftsemi").as[L#E]
    val filteredEdges = filteredEdgesSource
      .join(vertices, filteredEdgesSource.targetId === vertices(ColumnNames.ID), "leftsemi").as[L#E]
    graph.factory.init(graph.graphHead, vertices, filteredEdges)
  }
}
