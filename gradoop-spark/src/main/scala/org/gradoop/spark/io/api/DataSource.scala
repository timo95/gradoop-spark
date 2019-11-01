package org.gradoop.spark.io.api

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait DataSource {

  /** Reads the input as logical graph.
   *
   * @return logical graph
   */
  def getLogicalGraph: LogicalGraph[_, _, _, _, _]

  /** Reads the input as graph collection.
   *
   * @return graph collection
   */
  def getGraphCollection: GraphCollection[_, _, _, _, _]
}
