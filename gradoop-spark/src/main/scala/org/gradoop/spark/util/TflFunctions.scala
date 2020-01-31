package org.gradoop.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.gradoop.common.model.api.elements.MainElement
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.impl.types.Tfl

object TflFunctions {

  def induceRightMap[L <: MainElement, R <: MainElement](left: Map[String, Dataset[L]], right: Map[String, Dataset[R]])
    (implicit pEncoder: Encoder[R]): Map[String, Dataset[R]] = {
    joinMaps(left, right, "left")
      .map(p => (p._1, p._2.select(col(ColumnNames.ID),
        lit(p._1).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[R]))
  }

  def joinMaps[L <: MainElement, R <: MainElement](left: Map[String, Dataset[L]], right: Map[String, Dataset[R]],
    joinType: String): Map[String, DataFrame] = {
    left.map(e => (e._1, e._2.join(right(e._1), Seq(ColumnNames.ID, ColumnNames.LABEL), joinType)))
  }

  def splitGraphHeadMap[L <: Tfl[L]](graphHeadMap: Map[String, DataFrame])(implicit pEncoder: Encoder[L#P],
    gEncoder: Encoder[L#G]): (Map[String, Dataset[L#G]], Map[String, Dataset[L#P]]) = {
    // Split map in main element and property maps. Use constant as label.
    val resGrap = graphHeadMap
      .map(g => (g._1, g._2.select(col(ColumnNames.ID),
        lit(g._1).as(ColumnNames.LABEL)).as[L#G]))
    val resGrapProp = graphHeadMap
      .map(p => (p._1, p._2.select(col(ColumnNames.ID),
        lit(p._1).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[L#P]))
    (resGrap, resGrapProp)
  }

  def splitVertexMap[L <: Tfl[L]](vertexMap: Map[String, DataFrame])(implicit pEncoder: Encoder[L#P],
    vEncoder: Encoder[L#V]): (Map[String, Dataset[L#V]], Map[String, Dataset[L#P]]) = {
    // Split map in main element and property maps. Use constant as label.
    val resVert = vertexMap
      .map(v => (v._1, v._2.select(col(ColumnNames.ID),
        lit(v._1).as(ColumnNames.LABEL),
        col(ColumnNames.GRAPH_IDS)).as[L#V]))
    val resVertProp = vertexMap
      .map(p => (p._1, p._2.select(col(ColumnNames.ID),
        lit(p._1).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[L#P]))
    (resVert, resVertProp)
  }

  def splitEdgeMap[L <: Tfl[L]](edgeMap: Map[String, DataFrame])(implicit pEncoder: Encoder[L#P],
    eEncoder: Encoder[L#E]): (Map[String, Dataset[L#E]], Map[String, Dataset[L#P]]) = {
    // Split map in main element and property maps. Use constant as label.
    val resEdge = edgeMap
      .map(e => (e._1, e._2.select(col(ColumnNames.ID),
        lit(e._1).as(ColumnNames.LABEL),
        col(ColumnNames.SOURCE_ID),
        col(ColumnNames.TARGET_ID),
        col(ColumnNames.GRAPH_IDS)).as[L#E]))
    val resEdgeProp = edgeMap
      .map(p => (p._1, p._2.select(col(ColumnNames.ID),
        lit(p._1).as(ColumnNames.LABEL),
        col(ColumnNames.PROPERTIES)).as[L#P]))
    (resEdge, resEdgeProp)
  }
}
