package org.gradoop.spark.model.impl.operators.set.gve

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

trait GveSetBase {

  def removeUncontainedElements[EL <: GraphElement](elements: Dataset[EL], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Dataset[EL] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    val elementContainment = elements.select(elements.id, explode(elements.graphIds).as("graphId"))

    val remainingElementIds = elementContainment
      .join(graphIds.withColumnRenamed(ColumnNames.ID, "graphId"), "graphId")
      .select(ColumnNames.ID)
      .distinct

    elements.join(remainingElementIds, ColumnNames.ID).as[EL]
  }
}
