package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset

trait ElementAccess {

  /**
   * Returns all vertices.
   *
   * @return vertices
   */
  def getVertices: Dataset[V]

  /**
   * Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  def getVerticesByLabel(label: String): Dataset[V]

  /**
   * Returns all edges.
   *
   * @return edges
   */
  def getEdges: Dataset[E]

  /**
   * Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  def getEdgesByLabel(label: String): Dataset[E]
}
