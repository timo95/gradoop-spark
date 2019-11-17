package org.gradoop.spark.io.api

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.types.GveLayoutType

trait DataSink[L <: GveLayoutType] {

  /** Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   */
  def write(logicalGraph: LogicalGraph[L]): Unit = write(logicalGraph, SaveMode.ErrorIfExists)

  /** Writes a logical graph to the data sink with overwrite option.
   *
   * @param logicalGraph logical graph
   * @param saveMode     specifies, if existing files should be overwritten
   */
  def write(logicalGraph: LogicalGraph[L], saveMode: SaveMode): Unit

  /** Writes a graph collection graph to the data sink.
   *
   * @param graphCollection graph collection
   */
  def write(graphCollection: GraphCollection[L]): Unit = write(graphCollection, SaveMode.ErrorIfExists)

  /** Writes a graph collection to the data sink with overwrite option.
   *
   * @param graphCollection graph collection
   * @param saveMode        specifies, if existing files should be overwritten
   */
  def write(graphCollection: GraphCollection[L], saveMode: SaveMode): Unit
}
