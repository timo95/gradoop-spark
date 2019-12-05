package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayout, LogicalGraphLayout}
import org.gradoop.spark.model.impl.types.Tfl

abstract class TflLayout[L <: Tfl[L]](val graphHeads: Map[String, Dataset[L#G]],
                                      val vertices: Map[String, Dataset[L#V]],
                                      val edges: Map[String, Dataset[L#E]],
                                      val graphHeadProperties: Map[String, Dataset[L#P]],
                                      val vertexProperties: Map[String, Dataset[L#P]],
                                      val edgeProperties: Map[String, Dataset[L#P]])
  extends GraphCollectionLayout[L] with LogicalGraphLayout[L] {

  /**
   * Returns the graph heads associated with the logical graphs in that collection filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadsByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#G]): Dataset[L#G] =
    graphHeads.getOrElse(label, session.emptyDataset[L#G])

  /** Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def verticesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#V]): Dataset[L#V] =
    vertices.getOrElse(label, session.emptyDataset[L#V])

  /** Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def edgesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#E]): Dataset[L#E] =
    edges.getOrElse(label, session.emptyDataset[L#E])

  /**
   * Returns the graph head properties associated with the graph heads filtered by label.
   *
   * @param label graph head label
   * @return graph heads
   */
  def graphHeadPropertiesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#P]): Dataset[L#P] =
    graphHeadProperties.getOrElse(label, session.emptyDataset[L#P])

  /** Returns all vertex properties associated with the vertices filtered by label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def vertexPropertiesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#P]): Dataset[L#P] =
    vertexProperties.getOrElse(label, session.emptyDataset[L#P])

  /** Returns all edge properties associated with the edges filtered by label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def edgePropertiesByLabel(label: String)(implicit session: SparkSession, encoder: Encoder[L#P]): Dataset[L#P] =
    edgeProperties.getOrElse(label, session.emptyDataset[L#P])
}
