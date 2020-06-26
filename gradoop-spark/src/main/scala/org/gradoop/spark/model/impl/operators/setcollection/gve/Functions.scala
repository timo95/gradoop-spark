package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[gve] object Functions {

  def selectContainedElements[EL <: GraphElement](elements: Dataset[EL], graphIds: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Dataset[EL] = {

    elements.createOrReplaceTempView("elements")
    graphIds.createOrReplaceTempView("graphs")
    sparkSession
      .sql(s"SELECT * FROM elements LEFT SEMI JOIN graphs ON array_contains(elements.${ColumnNames.GRAPH_IDS}, graphs.${ColumnNames.ID})")
      .as[EL]
  }
}
