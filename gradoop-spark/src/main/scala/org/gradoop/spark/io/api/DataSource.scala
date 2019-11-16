package org.gradoop.spark.io.api

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait DataSource {

  /** Reads the input as logical graph.
   *
   * @return logical graph
   */
  def readLogicalGraph: LogicalGraph[_]

  /** Reads the input as graph collection.
   *
   * @return graph collection
   */
  def readGraphCollection: GraphCollection[_]
}
