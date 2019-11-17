package org.gradoop.spark.io.api

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.types.GveLayoutType

trait DataSource[L <: GveLayoutType] {

  /** Reads the input as logical graph.
   *
   * @return logical graph
   */
  def readLogicalGraph: LogicalGraph[L]

  /** Reads the input as graph collection.
   *
   * @return graph collection
   */
  def readGraphCollection: GraphCollection[L]
}
