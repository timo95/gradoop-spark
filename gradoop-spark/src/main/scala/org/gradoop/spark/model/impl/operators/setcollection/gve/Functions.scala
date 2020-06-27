package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames

private[gve] object Functions {

  def selectContainedElements[EL <: GraphElement](elements: Dataset[EL], graphs: DataFrame)
    (implicit sparkSession: SparkSession, encoder: Encoder[EL]): Dataset[EL] = {

    val EXPLODED_GRAPH_ID = "explodedGraphId"

    elements
      .withColumn(EXPLODED_GRAPH_ID, explode(col(ColumnNames.GRAPH_IDS)))
      .join(graphs, col(EXPLODED_GRAPH_ID) === graphs(ColumnNames.ID), "leftsemi")
      .drop(EXPLODED_GRAPH_ID).as[EL]

    /*
    elements.createOrReplaceTempView("elements")
    graphs.createOrReplaceTempView("graphs")
    sparkSession
      .sql(s"SELECT * FROM elements LEFT SEMI JOIN graphs ON array_contains(elements.${ColumnNames.GRAPH_IDS}, graphs.${ColumnNames.ID})")
      .as[EL]*/
  }
}
