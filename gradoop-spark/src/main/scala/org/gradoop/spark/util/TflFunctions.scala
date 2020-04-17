package org.gradoop.spark.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.components.Labeled
import org.gradoop.common.model.api.elements.MainElement
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.impl.types.Tfl

import scala.collection.mutable

object TflFunctions {

  // ----- General map functions -----

  /** Merge two maps while applying the given merge function. Outer. */
  def mergeMapsOuter[A](left: Map[String, A], right: Map[String, A], merge: (A, A) => A): Map[String, A] = {
    val result = mutable.Map(left.toSeq: _*)
    right.foreach(e => if (result.contains(e._1)) {
      result.update(e._1, merge(result(e._1), e._2))
    } else {
      result += e
    })
    result.toMap
  }

  /** Merge two maps while applying the given merge function. Inner. */
  def mergeMapsInner[L, R, O](left: Map[String, L], right: Map[String, R], merge: (L, R) => O): Map[String, O] = {
    left.flatMap(e => right.get(e._1) match {
      case Some(v) => Traversable((e._1, merge(e._2, v)))
      case None => Traversable.empty
    })
  }

  /** Merge two maps while applying the given merge function. Left. */
  def mergeMapsLeft[A](left: Map[String, A], right: Map[String, A], merge: (A, A) => A): Map[String, A] = {
    left.transform((k, v) => right.get(k) match {
      case Some(v2) => merge(v, v2)
      case None => v
    })
  }

  // ----- Tfl specific functions -----

  /** Apply union between two tfl maps. */
  def unionMaps[A](left: Map[String, Dataset[A]], right: Map[String, Dataset[A]]): Map[String, Dataset[A]] = {
    mergeMapsOuter[Dataset[A]](left, right, _ union _)
  }

  /** Reduces tfl map to single dataframe using union. TODO change usages: Select needed columns before reduceUnion
   *
   * Fails when dataFrames is empty. Prefer using datasets when possible.
   *
   * @param dataFrames iterable of dataframes
   * @param sparkSession spark session
   * @return union of dataframes
   */
  def reduceUnion(dataFrames: Iterable[DataFrame])(implicit sparkSession: SparkSession): DataFrame = {
    dataFrames.reduce(_ union _)
  }

  /** Reduces tfl map to single dataframe using union. */
  def reduceUnion[A](datasets: Iterable[Dataset[A]])(implicit sparkSession: SparkSession, encoder: Encoder[A]): Dataset[A] = {
    datasets.reduceOption(_ union _).getOrElse(sparkSession.emptyDataset[A])
  }

  /** Remove properties of missing elements. */
  def inducePropMap[EL <: MainElement, P <: MainElement](element: Map[String, Dataset[EL]], prop: Map[String, Dataset[P]])
    (implicit pEncoder: Encoder[P]): Map[String, Dataset[P]] = {
    val elem = element.mapValues(_.withColumnRenamed(ColumnNames.ID, "e_id").as("elem"))

    elem.transform((k, v) => {
      prop(k).as("prop").join(v,
        col(s"prop.${ColumnNames.ID}") === col(s"elem.e_id"), "leftsemi")
        .as[P]
    })
  }

  /** Join property map to element map. */
  def joinPropMap[EL <: MainElement, P <: MainElement](left: Map[String, Dataset[EL]], right: Map[String, Dataset[P]],
    joinType: String): Map[String, DataFrame] = {
    left.transform((k, v) => {
      // Workaround for selfjoin. Alias or joinWith alone don't work, both are needed.
      val prop = right(k).drop(ColumnNames.LABEL)
      val propCols = (prop.columns.toSet - ColumnNames.ID).map(s => col("_2." + s))
      val cols = v.columns.map(s => col("_1." + s)) ++ propCols
      v.as("l").joinWith(prop.as("r"),
        col(s"l.${ColumnNames.ID}") === col(s"r.${ColumnNames.ID}"), joinType)
        .select(cols: _*)
    })
  }

  /** Split map in element map and property map. */
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

  /** Split map in element map and property map. */
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

  /** Split map in element map and property map. */
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

  /** Verify that datasets only contain the map key as label. */
  def verifyLabels[EL <: Labeled](map: Map[String, Dataset[EL]]): Map[String, Dataset[EL]] = {
    val cached = map.mapValues(_.cache)
    cached.foreach(e => {
      val filtered = e._2.filter(not(FilterExpressions.hasLabel(e._1)))
      if(!filtered.isEmpty) {
        val label = filtered.collect.head.label
        throw new IllegalStateException("Tfl Dataset for label '%s' contains wrong label '%s'.".format(e._1, label))
      }
    })
    cached
  }
}
