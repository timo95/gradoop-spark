package org.gradoop.spark.model.impl.operators.removedanglingedges.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveRemoveDanglingEdges[L <: Gve[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    import graph.config.Implicits._
    val factory = graph.factory
    import factory.Implicits._

    val filteredEdgesSource = graph.edges
      .join(graph.vertices, graph.edges.sourceId === graph.vertices(ColumnNames.ID), "leftsemi").as[L#E]
    val filteredEdges = filteredEdgesSource
      .join(graph.vertices, filteredEdgesSource.targetId === graph.vertices(ColumnNames.ID), "leftsemi").as[L#E]
    graph.factory.init(graph.graphHead, graph.vertices, filteredEdges)
  }
}
