package org.gradoop.spark.model.impl.operators.verify.tfl

import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflVerify[L <: Tfl[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    implicit val sparkSession = graph.config.sparkSession
    import sparkSession.implicits._

    val vertexUnion = TflFunctions.reduceUnion(graph.vertices.values)

    val verifiedEdgesSource = graph.edges.mapValues(e =>
      e.joinWith(vertexUnion, e.sourceId === vertexUnion.id).map(_._1))
    val verifiedEdges = verifiedEdgesSource.mapValues(e =>
      e.joinWith(vertexUnion, e.targetId === vertexUnion.id).map(_._1))

    graph.factory.init(graph.graphHead, graph.vertices, verifiedEdges, graph.graphHeadProperties,
      graph.vertexProperties, TflFunctions.inducePropMap(verifiedEdges, graph.edgeProperties))
  }
}
