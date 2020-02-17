package org.gradoop.spark.model.impl.operators.equality.gve

import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToValueOperator
import org.gradoop.spark.model.impl.types.Gve

class GveGraphCollectionEqualityByGraphIds[L <: Gve[L]] extends BinaryGraphCollectionToValueOperator[L#GC, Boolean] {

  override def execute(left: L#GC, right: L#GC): Boolean = {
    val config = left.config
    import config.Implicits._
    import config.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val LEFT = "left"
    val RIGHT = "right"

    val leftIds = left.graphHeads.select(left.graphHeads.id.as(LEFT)).dropDuplicates
    val rightIds = right.graphHeads.select(right.graphHeads.id.as(RIGHT)).dropDuplicates

    leftIds.join(rightIds, leftIds(LEFT) === rightIds(RIGHT), "outer")
      .where(isnull(col(LEFT)) || isnull(col(RIGHT)))
      .isEmpty
  }
}
