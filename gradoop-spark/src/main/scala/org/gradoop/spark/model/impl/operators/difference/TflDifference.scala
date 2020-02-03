package org.gradoop.spark.model.impl.operators.difference

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.GraphElement
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflDifference[L <: Tfl[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession
    import sparkSession.implicits._

    val leftGraphIds = left.graphHeads.mapValues(v => v.select(v.id))
    val rightGraphIdsUnion = right.graphHeads.values.map(v => v.select(v.id)).reduce(_ union _)

    val remainingIds = leftGraphIds.mapValues(v => v.except(rightGraphIdsUnion))
    val resGraphHeads = left.graphHeads.map(e =>
      (e._1, e._2.join(remainingIds(e._1), ColumnNames.ID).as[L#G]))

    val remainingIdsUnion = remainingIds.values.reduce(_ union _)
    val resVertices = removeUncontainedElements(left.vertices, remainingIdsUnion)
    val resEdges = removeUncontainedElements(left.edges, remainingIdsUnion)

    left.factory.init(resGraphHeads, resVertices, resEdges,
      TflFunctions.induceRightMap(resGraphHeads, left.graphHeadProperties),
      TflFunctions.induceRightMap(resVertices, left.vertexProperties),
      TflFunctions.induceRightMap(resEdges, left.edgeProperties),
    )
  }

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

    elements.map(e => (e._1, e._2.join(remainingElementIds(e._1), ColumnNames.ID).as[EL]))
  }
}
