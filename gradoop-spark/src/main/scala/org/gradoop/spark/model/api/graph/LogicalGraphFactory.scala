package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.layouts.LogicalGraphLayoutFactory

class LogicalGraphFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection]
  (var layoutFactory: LogicalGraphLayoutFactory[G, V, E, LG, GC]) {

  /**
   * Get the layout factory responsible for creating a graph layout.
   *
   * @return The graph layout factory.
   */
  implicit def getLayoutFactory(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LogicalGraphLayoutFactory[G, V, E, LG, GC] = layoutFactory

  /**
   * Sets the layout factory that is responsible for creating a graph layout.
   *
   * @param layoutFactory graph layout factory
   */
  def setLayoutFactory(layoutFactory: LogicalGraphLayoutFactory[G, V, E, LG, GC]): Unit = {
    this.layoutFactory = layoutFactory
  }
}
