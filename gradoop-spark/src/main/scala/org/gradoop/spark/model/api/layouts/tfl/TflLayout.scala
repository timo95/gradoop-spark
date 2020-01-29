package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayout, LogicalGraphLayout}
import org.gradoop.spark.model.impl.types.Tfl

abstract class TflLayout[L <: Tfl[L]](val graphHeads: Map[String, Dataset[L#G]],
                                      val vertices: Map[String, Dataset[L#V]],
                                      val edges: Map[String, Dataset[L#E]],
                                      val graphHeadProperties: Map[String, Dataset[L#P]],
                                      val vertexProperties: Map[String, Dataset[L#P]],
                                      val edgeProperties: Map[String, Dataset[L#P]])
  extends GraphCollectionLayout[L] with LogicalGraphLayout[L] {

  def graphHead: Map[String, Dataset[L#G]] = graphHeads

  /** Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return label to graph head map
   */
  def graphHeadsByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#G]): Dataset[L#G] = {
    graphHeads.getOrElse(label, session.emptyDataset[L#G])
  }

  /** Returns the graph heads combined with their properties.
   *
   * @return label to graph head map
   */
  def graphHeadsWithProperties: Map[String, DataFrame] = {
    graphHeads.map(g => (g._1, g._2.join(graphHeadProperties(g._1), Seq(ColumnNames.ID, ColumnNames.LABEL))))
  }

  /** Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def verticesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#V]): Dataset[L#V] = {
    vertices.getOrElse(label, session.emptyDataset[L#V])
  }

  /** Returns the vertices with their properties.
   *
   * @return label to vertex map
   */
  def verticesWithProperties: Map[String, DataFrame] = {
    vertices.map(v => (v._1, v._2.join(vertexProperties(v._1), Seq(ColumnNames.ID, ColumnNames.LABEL))))
  }

  /** Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def edgesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#E]): Dataset[L#E] = {
    edges.getOrElse(label, session.emptyDataset[L#E])
  }

  /** Returns the edges with their properties.
   *
   * @return label to edge map
   */
  def edgesWithProperties: Map[String, DataFrame] = {
    edges.map(e => (e._1, e._2.join(edgeProperties(e._1), Seq(ColumnNames.ID, ColumnNames.LABEL))))
  }

  /** Returns the graph head properties associated with the graph heads filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadPropertiesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#P]): Dataset[L#P] = {
    graphHeadProperties.getOrElse(label, session.emptyDataset[L#P])
  }

  /** Returns all vertex properties associated with the vertices filtered by label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def vertexPropertiesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#P]): Dataset[L#P] = {
    vertexProperties.getOrElse(label, session.emptyDataset[L#P])
  }

  /** Returns all edge properties associated with the edges filtered by label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def edgePropertiesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#P]): Dataset[L#P] = {
    edgeProperties.getOrElse(label, session.emptyDataset[L#P])
  }
}
