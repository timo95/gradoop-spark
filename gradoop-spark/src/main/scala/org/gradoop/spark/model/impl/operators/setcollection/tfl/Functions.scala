package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.components.Identifiable
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[tfl] object Functions {

  def removeUncontainedElements[EL <: GraphElement](elements: Map[String, Dataset[EL]], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Map[String, Dataset[EL]] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import sparkSession.implicits._

    val containment = elements
      .mapValues(e => e.select(col(ColumnNames.ID).as("newIds"), explode(e.graphIds).as("graphId")))

    val newGraphIds = graphIds.withColumnRenamed(ColumnNames.ID, "newGraphId")

    val newElementIds = containment.mapValues(
      _.join(newGraphIds, col("graphId") === col("newGraphId"), "leftsemi")
        .select(col("newIds"))
        .distinct)

    elements.transform((k, v) => v.join(newElementIds(k), col(ColumnNames.ID) === col("newIds"), "leftsemi").as[EL])
  }
}
