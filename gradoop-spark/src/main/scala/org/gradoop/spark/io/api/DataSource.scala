package org.gradoop.spark.io.api

import org.gradoop.spark.model.impl.types.Gve

trait DataSource[L <: Gve[L]] {

  /** Reads the input as logical graph.
   *
   * @return logical graph
   */
  def readLogicalGraph: L#LG

  /** Reads the input as graph collection.
   *
   * @return graph collection
   */
  def readGraphCollection: L#GC
}
