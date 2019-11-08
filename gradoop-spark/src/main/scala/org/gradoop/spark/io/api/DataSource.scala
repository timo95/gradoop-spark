package org.gradoop.spark.io.api

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait DataSource {

  /** Reads the input as logical graph.
   *
   * @return logical graph
   */
  def readLogicalGraph: LogicalGraph[_, _, _, _, _]

  /** Reads the input as graph collection.
   *
   * @return graph collection
   */
  def readGraphCollection: GraphCollection[_, _, _, _, _]
}
