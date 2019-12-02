package org.gradoop.spark.model.impl.operators.verify

import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.GveLayoutType

class Verify[L <: GveLayoutType[L]] extends LogicalGraphToLogicalGraphOperator[LogicalGraph[L]] {

  override def execute(graph: LogicalGraph[L]): LogicalGraph[L] = {
    val factory = graph.factory
    import factory.implicits._
    import graph.config.sparkSession.implicits._
    val verifiedEdgesSource = graph.edges
      .joinWith(graph.vertices, graph.edges.sourceId === graph.vertices.id)
      .map(t => t._1)
    val verifiedEdges = verifiedEdgesSource
      .joinWith(graph.vertices, verifiedEdgesSource.targetId === graph.vertices.id)
      .map(t => t._1)
    graph.factory.init(graph.graphHead, graph.vertices, verifiedEdges)
  }
}
