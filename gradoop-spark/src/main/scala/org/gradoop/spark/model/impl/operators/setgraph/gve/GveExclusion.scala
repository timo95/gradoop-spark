package org.gradoop.spark.model.impl.operators.setgraph.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveExclusion[L <: Gve[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._

    val remainingVertexIds = left.vertices.select(ColumnNames.ID)
      .except(right.vertices.select(ColumnNames.ID)).dropDuplicates(ColumnNames.ID)
    val resVertices = left.vertices.join(remainingVertexIds, ColumnNames.ID).as[L#V]

    val remainingEdgeIds = left.edges.select(ColumnNames.ID)
      .except(right.edges.select(ColumnNames.ID)).dropDuplicates(ColumnNames.ID)
    val resEdges = left.edges.join(remainingEdgeIds, ColumnNames.ID).as[L#E]

    factory.init(left.graphHeads, resVertices, resEdges).verify
  }
}
