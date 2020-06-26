package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[tfl] object Functions {

  def selectContainedElements[EL <: GraphElement](elements: Map[String, Dataset[EL]], graphs: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Map[String, Dataset[EL]] = {

    graphs.createOrReplaceTempView("graphs")
    elements.mapValues(df => {
      df.createOrReplaceTempView("elements")
      sparkSession
        .sql(s"SELECT * FROM elements LEFT SEMI JOIN graphs ON array_contains(elements.${ColumnNames.GRAPH_IDS}, graphs.${ColumnNames.ID})")
        .as[EL]
    })
  }
}
