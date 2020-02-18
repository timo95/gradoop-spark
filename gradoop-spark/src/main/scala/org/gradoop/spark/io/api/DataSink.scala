package org.gradoop.spark.io.api

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.model.impl.types.LayoutType

trait DataSink[L <: LayoutType[L]] {

  /** Writes a logical graph to the data sink.
   *
   * @param logicalGraph logical graph
   */
  def write(logicalGraph: L#LG): Unit = write(logicalGraph, SaveMode.ErrorIfExists)

  /** Writes a logical graph to the data sink with overwrite option.
   *
   * @param logicalGraph logical graph
   * @param saveMode     specifies, if existing files should be overwritten
   */
  def write(logicalGraph: L#LG, saveMode: SaveMode): Unit

  /** Writes a graph collection graph to the data sink.
   *
   * @param graphCollection graph collection
   */
  def write(graphCollection: L#GC): Unit = write(graphCollection, SaveMode.ErrorIfExists)

  /** Writes a graph collection to the data sink with overwrite option.
   *
   * @param graphCollection graph collection
   * @param saveMode        specifies, if existing files should be overwritten
   */
  def write(graphCollection: L#GC, saveMode: SaveMode): Unit
}
