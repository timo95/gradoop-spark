package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

trait TflSetBase {

  def removeUncontainedElements[EL <: GraphElement](elements: Map[String, Dataset[EL]], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Map[String, Dataset[EL]] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    val elementContainment = elements
      .mapValues(e => e.select(e.id, explode(e.graphIds).as("graphId")))

    val remainingElementIds = elementContainment.mapValues(e =>
      e.join(graphIds.withColumnRenamed(ColumnNames.ID, "graphId"), "graphId")
        .select(ColumnNames.ID).distinct)

    elements.transform((k, v) => v.join(remainingElementIds(k), ColumnNames.ID).as[EL])
  }
}
