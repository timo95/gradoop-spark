package org.gradoop.spark.model.impl.operators.set.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.types.Gve

class GveDifference[L <: Gve[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] with GveSetBase {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession
    import sparkSession.implicits._

    val leftGraphIds = left.graphHeads.select(left.graphHeads.id)
    val rightGraphIds = right.graphHeads.select(right.graphHeads.id)

    // difference on graph head ids
    val differenceIds = leftGraphIds.except(rightGraphIds)
    val graphHeads = left.graphHeads.join(differenceIds, ColumnNames.ID).as[L#G]

    left.factory.init(graphHeads,
      removeUncontainedElements(left.vertices, differenceIds),
      removeUncontainedElements(left.edges, differenceIds))
  }
}
