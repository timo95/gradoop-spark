package org.gradoop.spark.model.impl.operators.setgraph.gve

import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveOverlap[L <: Gve[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._
    import left.config.Implicits._

    val resVertices = left.vertices
      .join(right.vertices, left.vertices.id === right.vertices.id, "leftsemi").as[L#V]

    val resEdges = left.edges
      .join(right.edges, left.edges.id === right.edges.id, "leftsemi").as[L#E]

    factory.init(left.graphHeads, resVertices, resEdges)
  }
}
