package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.gve.{GveEdge, GveVertex}
import org.gradoop.spark.expressions.filter.FilterStrings

trait ElementAccess[V <: GveVertex, E <: GveEdge] {

  /** Returns all vertices.
   *
   * @return vertices
   */
  def vertices: Dataset[V]

  /** Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def verticesByLabel(label: String): Dataset[V] = vertices.filter(FilterStrings.hasLabel(label))

  /** Returns all edges.
   *
   * @return edges
   */
  def edges: Dataset[E]

  /** Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def edgesByLabel(label: String): Dataset[E] = edges.filter(FilterStrings.hasLabel(label))
}
