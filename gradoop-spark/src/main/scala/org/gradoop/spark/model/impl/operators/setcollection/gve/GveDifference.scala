package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.operators.setcollection.gve.Functions.removeUncontainedElements
import org.gradoop.spark.model.impl.types.Gve

class GveDifference[L <: Gve[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession

    val leftGraphIds = left.graphHeads.select(ColumnNames.ID)
    val rightGraphIds = right.graphHeads.select(ColumnNames.ID)

    // difference on graph head ids
    val differenceIds = leftGraphIds.except(rightGraphIds)
    val graphHeads = left.graphHeads.join(differenceIds, ColumnNames.ID).as[L#G]

    left.factory.init(graphHeads,
      removeUncontainedElements(left.vertices, differenceIds),
      removeUncontainedElements(left.edges, differenceIds))
  }
}
