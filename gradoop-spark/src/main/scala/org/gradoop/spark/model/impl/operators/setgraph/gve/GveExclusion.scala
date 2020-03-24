package org.gradoop.spark.model.impl.operators.setgraph.gve

import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveExclusion[L <: Gve[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._
    import left.config.Implicits._

    val vertices = left.vertices.join(right.vertices, left.vertices.id === right.vertices.id, "leftanti").as[L#V]
    val edges = left.edges.join(right.edges, left.edges.id === right.edges.id, "leftanti").as[L#E]

    factory.init(left.graphHeads, vertices, edges).removeDanglingEdges
  }
}
