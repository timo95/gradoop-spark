package org.gradoop.spark.model.api.config

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}

abstract class GradoopSparkConfig[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection]()(implicit session: SparkSession) {

  // Hold everything as members?
  // Convert to other format by passing config?
  /**
   * Creates instances of {@link LogicalGraph}
   */
  var logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC]

  /**
   * Creates instances of {@link GraphCollection}
   */
  var graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC]


  def getLogicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC] = logicalGraphFactory
  def getGraphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC] = graphCollectionFactory
}
