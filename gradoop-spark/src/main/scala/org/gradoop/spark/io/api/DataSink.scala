package org.gradoop.spark.io.api

import org.apache.spark.sql.SaveMode
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait DataSink[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]] {

  /** Writes a logical graph to the data sink with overwrite option.
   *
   * @param logicalGraph logical graph
   * @param saveMode     specifies, if existing files should be overwritten
   */
  def write(logicalGraph: LG, saveMode: SaveMode): Unit

  /** Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   */
  def write(logicalGraph: LG): Unit

  /** Writes a graph collection graph to the data sink.
   *
   * @param graphCollection graph collection
   */
  def write(graphCollection: GC): Unit

  /** Writes a graph collection to the data sink with overwrite option.
   *
   * @param graphCollection graph collection
   * @param saveMode        specifies, if existing files should be overwritten
   */
  def write(graphCollection: GC, saveMode: SaveMode): Unit
}
