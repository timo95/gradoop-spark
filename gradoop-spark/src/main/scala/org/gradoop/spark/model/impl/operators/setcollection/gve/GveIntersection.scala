package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.operators.setcollection.gve.Functions.removeUncontainedElements
import org.gradoop.spark.model.impl.types.Gve

class GveIntersection[L <: Gve[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    import left.config.Implicits._

    val graphHeads = left.graphHeads.join(right.graphHeads, left.graphHeads.id === right.graphHeads.id, "leftsemi").as[L#G]

    factory.init(graphHeads,
      removeUncontainedElements(left.vertices, graphHeads.toDF),
      removeUncontainedElements(left.edges, graphHeads.toDF))
  }
}
