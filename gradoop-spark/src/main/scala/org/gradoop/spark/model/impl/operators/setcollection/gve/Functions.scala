package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.components.Identifiable
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[gve] object Functions {

  def removeUncontainedElements[EL <: GraphElement](elements: Dataset[EL], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Dataset[EL] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    // id, graphId mapping of elements
    val containment = elements
      .select(col(ColumnNames.ID).as("newIds"), explode(elements.graphIds).as("graphId"))

    // graphIds to induce by
    val newGraphIds = graphIds.withColumnRenamed(ColumnNames.ID, "newGraphId")

    // induced element ids
    val newElementIds = containment
      .join(newGraphIds, col("graphId") === col("newGraphId"), "leftsemi")
      .select(col("newIds"))
      .distinct

    // induced elements
    elements.join(newElementIds, col(ColumnNames.ID) === col("newIds"), "leftsemi").as[EL]
  }
}
