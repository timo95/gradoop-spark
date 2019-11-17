package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.gve.{Edge, Vertex}
import org.gradoop.spark.functions.filter.HasLabel

trait ElementAccess[V <: Vertex, E <: Edge] {

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
  def verticesByLabel(label: String): Dataset[V] = vertices.filter(new HasLabel[V](label))

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
  def edgesByLabel(label: String): Dataset[E] = edges.filter(new HasLabel[E](label))
}
