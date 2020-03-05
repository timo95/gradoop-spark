package org.gradoop.spark.model.impl.operators.removedanglingedges.tfl

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflRemoveDanglingEdges[L <: Tfl[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val factory = graph.factory
    import factory.Implicits._
    implicit val sparkSession = graph.config.sparkSession
    import sparkSession.implicits._

    val vertexIdUnion = TflFunctions.reduceUnion(graph.vertices.values
      .map(_.select(ColumnNames.ID).cache))

    val filteredEdgesSource = graph.edges.mapValues(e =>
      e.joinWith(vertexIdUnion, e.sourceId === vertexIdUnion(ColumnNames.ID)).select("_1.*").as[L#E])
    val filteredEdges = filteredEdgesSource.mapValues(e =>
      e.joinWith(vertexIdUnion, e.targetId === vertexIdUnion(ColumnNames.ID)).select("_1.*").as[L#E])

    graph.factory.init(graph.graphHead, graph.vertices, filteredEdges, graph.graphHeadProperties,
      graph.vertexProperties, TflFunctions.inducePropMap(filteredEdges, graph.edgeProperties))
  }
}
