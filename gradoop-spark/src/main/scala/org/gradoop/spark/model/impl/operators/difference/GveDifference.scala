package org.gradoop.spark.model.impl.operators.difference

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.types.Gve

class GveDifference[L <: Gve[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession
    import sparkSession.implicits._

    val leftGraphIds = left.graphHeads.select(left.graphHeads.id)
    val rightGraphIds = right.graphHeads.select(right.graphHeads.id)

    // difference on graph head ids
    val remainingIds = leftGraphIds.except(rightGraphIds)
    val graphHeads = left.graphHeads.join(remainingIds, ColumnNames.ID).as[L#G]

    left.factory.init(graphHeads,
      removeUncontainedElements(left.vertices, remainingIds),
      removeUncontainedElements(left.edges, remainingIds))
  }

  def removeUncontainedElements[EL <: GraphElement](elements: Dataset[EL], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Dataset[EL] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    val elementContainment = elements.select(elements.id, explode(elements.graphIds).as("graphId"))

    val remainingIds = elementContainment
      .join(graphIds.withColumnRenamed(ColumnNames.ID, "graphId"), "graphId")
      .select(ColumnNames.ID)
      .distinct

    elements.join(remainingIds, ColumnNames.ID).as[EL]
  }
}
