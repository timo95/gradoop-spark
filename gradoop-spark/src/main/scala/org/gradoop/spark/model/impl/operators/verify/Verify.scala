package org.gradoop.spark.model.impl.operators.verify

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator

class Verify[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
  extends LogicalGraphToLogicalGraphOperator[LG] {

  override def execute(graph: LG): LG = {
    import graph.config.implicits._
    val verifiedEdgesSource = graph.edges
      .joinWith(graph.vertices, graph.edges("sourceId") === graph.vertices("id"))
      .map(t => t._1)
    val verifiedEdges = verifiedEdgesSource
      .joinWith(graph.vertices, verifiedEdgesSource("targetId") === graph.vertices("id"))
      .map(t => t._1)
    graph.factory.init(graph.graphHead, graph.vertices, verifiedEdges)
  }
}
