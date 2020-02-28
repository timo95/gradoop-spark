package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[tfl] object Functions {

  def removeUncontainedElements[EL <: GraphElement](elements: Map[String, Dataset[EL]], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Map[String, Dataset[EL]] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    val GRAPH_ID = "graphId"

    val elementContainment = elements
      .mapValues(e => e.select(col(ColumnNames.ID), explode(e.graphIds).as(GRAPH_ID)))

    val remainingElementIds = elementContainment.mapValues(
      _.join(graphIds.withColumnRenamed(ColumnNames.ID, GRAPH_ID), GRAPH_ID)
        .select(ColumnNames.ID)
        .distinct)

    elements.transform((k, v) => v.join(remainingElementIds(k), ColumnNames.ID).as[EL])
  }
}
