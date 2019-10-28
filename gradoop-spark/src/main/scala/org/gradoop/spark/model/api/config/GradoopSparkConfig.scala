package org.gradoop.spark.model.api.config

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}

class GradoopSparkConfig [
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(var logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC],
 var graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC])
(implicit sparkSession: SparkSession) {

  def getLogicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC] = logicalGraphFactory

  def getGraphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC] = graphCollectionFactory

  def getSparkSession: SparkSession = sparkSession

  /** Sets the layout factory that is responsible for creating a logical graph layout.
   *
   * @param logicalGraphFactory logical graph layout factory
   */
  def setLogicalGraphFactory(logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC]): Unit = {
    this.logicalGraphFactory = logicalGraphFactory
  }

  /** Sets the layout factory that is responsible for creating a graph collection layout.
   *
   * @param graphCollectionFactory graph collection layout factory
   */
  def setGraphCollectionFactory(graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC]): Unit = {
    this.graphCollectionFactory = graphCollectionFactory
  }
}
