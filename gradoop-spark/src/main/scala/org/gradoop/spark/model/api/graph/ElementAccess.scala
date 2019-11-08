package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, Vertex}

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
  def verticesByLabel(label: String): Dataset[V]

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
  def edgesByLabel(label: String): Dataset[E]
}
