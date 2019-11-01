package org.gradoop.spark.io.api

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait DataSink {

  /** Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   */
  def write(logicalGraph: LogicalGraph[_, _, _, _, _]): Unit

  /** Writes a graph collection graph to the data sink.
   *
   * @param graphCollection graph collection
   */
  def write(graphCollection: GraphCollection[_, _, _, _, _]): Unit

  /** Writes a logical graph to the data sink with overwrite option.
   *
   * @param logicalGraph logical graph
   * @param overwrite    true, if existing files should be overwritten
   */
  def write(logicalGraph: LogicalGraph[_, _, _, _, _], overwrite: Boolean): Unit

  /** Writes a graph collection to the data sink with overwrite option.
   *
   * @param graphCollection graph collection
   * @param overwrite       true, if existing files should be overwritten
   */
  def write(graphCollection: GraphCollection[_, _, _, _, _], overwrite: Boolean): Unit

}
