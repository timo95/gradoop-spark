package org.gradoop.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.MainElement
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.impl.types.Tfl

import scala.collection.mutable

object TflFunctions {

  // General map functions

  def mergeMaps[A](left: Map[String, A], right: Map[String, A], merge: (A, A) => A): Map[String, A] = {
    val result = mutable.Map(left.toSeq: _*)
    right.foreach(e => if (result.contains(e._1)) {
      result.update(e._1, merge(result(e._1), e._2))
    } else {
      result += e
    })
    result.toMap
  }

  def unionMaps[A](left: Map[String, Dataset[A]], right: Map[String, Dataset[A]]): Map[String, Dataset[A]] = {
    mergeMaps[Dataset[A]](left, right, _ union _)
  }

  def reduceUnion(it: Iterable[DataFrame])(implicit sparkSession: SparkSession): DataFrame = {
    it.reduce(_ union _) // TODO find way to keep row data (for empty DF) or eliminate uses
  }

  def reduceUnion[A](it: Iterable[Dataset[A]])(implicit sparkSession: SparkSession, encoder: Encoder[A]): Dataset[A] = {
    it.reduceOption(_ union _).getOrElse(sparkSession.emptyDataset[A])
  }

  // Properties map functions

  def inducePropMap[EL <: MainElement, P <: MainElement](left: Map[String, Dataset[EL]], right: Map[String, Dataset[P]])
    (implicit pEncoder: Encoder[P]): Map[String, Dataset[P]] = {
    joinPropMap(left, right, "left").transform((k, v) => v.select(col(ColumnNames.ID),
      lit(k).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)).as[P])
  }

  def joinPropMap[EL <: MainElement, P <: MainElement](left: Map[String, Dataset[EL]], right: Map[String, Dataset[P]],
    joinType: String): Map[String, DataFrame] = {
    left.transform((k, v) => v.join(right(k), Seq(ColumnNames.ID, ColumnNames.LABEL), joinType))
  }

  def splitGraphHeadMap[L <: Tfl[L]](graphHeadMap: Map[String, DataFrame])(implicit pEncoder: Encoder[L#P],
    gEncoder: Encoder[L#G]): (Map[String, Dataset[L#G]], Map[String, Dataset[L#P]]) = {
    // Split map in main element and property maps. Use constant as label.
    val resGrap = graphHeadMap
      .transform((k, v) => v.select(col(ColumnNames.ID),
        lit(k).as(ColumnNames.LABEL)).as[L#G])
    val resGrapProp = graphHeadMap
      .transform((k, v) => v.select(col(ColumnNames.ID),
        lit(k).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[L#P])
    (resGrap, resGrapProp)
  }

  def splitVertexMap[L <: Tfl[L]](vertexMap: Map[String, DataFrame])(implicit pEncoder: Encoder[L#P],
    vEncoder: Encoder[L#V]): (Map[String, Dataset[L#V]], Map[String, Dataset[L#P]]) = {
    // Split map in main element and property maps. Use constant as label.
    val resVert = vertexMap
      .transform((k, v) => v.select(col(ColumnNames.ID),
        lit(k).as(ColumnNames.LABEL),
        col(ColumnNames.GRAPH_IDS)).as[L#V])
    val resVertProp = vertexMap
      .transform((k, v) => v.select(col(ColumnNames.ID),
        lit(k).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[L#P])
    (resVert, resVertProp)
  }

  def splitEdgeMap[L <: Tfl[L]](edgeMap: Map[String, DataFrame])(implicit pEncoder: Encoder[L#P],
    eEncoder: Encoder[L#E]): (Map[String, Dataset[L#E]], Map[String, Dataset[L#P]]) = {
    // Split map in main element and property maps. Use constant as label.
    val resEdge = edgeMap
      .transform((k, v) => v.select(col(ColumnNames.ID),
        lit(k).as(ColumnNames.LABEL),
        col(ColumnNames.SOURCE_ID),
        col(ColumnNames.TARGET_ID),
        col(ColumnNames.GRAPH_IDS)).as[L#E])
    val resEdgeProp = edgeMap
      .transform((k, v) => v.select(col(ColumnNames.ID),
        lit(k).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[L#P])
    (resEdge, resEdgeProp)
  }
}
