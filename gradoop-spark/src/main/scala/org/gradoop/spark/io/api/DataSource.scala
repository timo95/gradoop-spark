package org.gradoop.spark.io.api

import org.gradoop.spark.model.impl.types.LayoutType

trait DataSource[L <: LayoutType[L]] {

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
