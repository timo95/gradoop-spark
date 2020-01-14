package org.gradoop.spark.model.impl.operators.equality.gve

import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToValueOperator
import org.gradoop.spark.model.impl.types.Gve

class GveGraphCollectionEqualityByGraphIds[L <: Gve[L]] extends BinaryGraphCollectionToValueOperator[L#GC, Boolean] {

  override def execute(left: L#GC, right: L#GC): Boolean = {
    val config = left.config
    import config.Implicits._
    import config.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val leftIds = left.graphHeads.select(left.graphHeads.id).dropDuplicates
      .withColumnRenamed("id", "left")
    val rightIds = right.graphHeads.select(right.graphHeads.id).dropDuplicates
      .withColumnRenamed("id", "right")

    leftIds.join(rightIds, leftIds("left") === rightIds("right"), "outer")
      .where(isnull(col("left")) || isnull(col("right")))
      .isEmpty
  }
}
