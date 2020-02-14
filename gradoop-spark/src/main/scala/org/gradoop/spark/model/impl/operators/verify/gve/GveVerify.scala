package org.gradoop.spark.model.impl.operators.verify.gve

import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveVerify[L <: Gve[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    import graph.config.sparkSession.implicits._
    val verifiedEdgesSource = graph.edges
      .joinWith(graph.vertices, graph.edges.sourceId === graph.vertices.id)
      .select("_1.*").as[L#E]
    val verifiedEdges = verifiedEdgesSource
      .joinWith(graph.vertices, verifiedEdgesSource.targetId === graph.vertices.id)
      .select("_1.*").as[L#E]
    graph.factory.init(graph.graphHead, graph.vertices, verifiedEdges)
  }
}
