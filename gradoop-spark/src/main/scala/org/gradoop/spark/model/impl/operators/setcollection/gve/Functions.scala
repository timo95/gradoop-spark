package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[gve] object Functions {

  def removeUncontainedElements[EL <: GraphElement](elements: Dataset[EL], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Dataset[EL] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    val GRAPH_ID = "graphId"

    val elementContainment = elements.select(col(ColumnNames.ID), explode(elements.graphIds).as(GRAPH_ID))

    val remainingElementIds = elementContainment
      .join(graphIds.withColumnRenamed(ColumnNames.ID, GRAPH_ID), GRAPH_ID)
      .select(ColumnNames.ID)
      .distinct

    elements.join(remainingElementIds, ColumnNames.ID).as[EL]
  }
}
