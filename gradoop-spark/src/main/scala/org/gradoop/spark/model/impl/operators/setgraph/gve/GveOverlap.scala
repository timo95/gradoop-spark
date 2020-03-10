package org.gradoop.spark.model.impl.operators.setgraph.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveOverlap[L <: Gve[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._
    import left.config.Implicits._

    val rightVertexIds = right.vertices.select(ColumnNames.ID)
    val resVertices = left.vertices
      .join(rightVertexIds, ColumnNames.ID).as[L#V]

    val rightEdgeIds = right.edges.select(ColumnNames.ID)
    val resEdges = left.edges
      .join(rightEdgeIds, ColumnNames.ID).as[L#E]

    factory.init(left.graphHeads, resVertices, resEdges)
  }
}
