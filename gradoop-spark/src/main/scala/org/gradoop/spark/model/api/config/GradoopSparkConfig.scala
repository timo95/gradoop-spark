package org.gradoop.spark.model.api.config

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}

class GradoopSparkConfig[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(var logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC], var graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC])
(implicit sparkSession: SparkSession) {

  def getLogicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC] = logicalGraphFactory
  def getGraphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC] = graphCollectionFactory

  def getSparkSession: SparkSession = sparkSession

  def getGraphHeadEncoder: Encoder[G] = Encoders.kryo[G]
  def getVertexEncoder: Encoder[V] = Encoders.kryo[V]
  def getEdgeEncoder: Encoder[E] = Encoders.kryo[E]

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

object GradoopSparkConfig {

  def apply[G, V, E, LG, GC](logicalGraphLayoutFactory: LogicalGraphLayoutFactory[G, V, E],
                             graphCollectionLayoutFactory: GraphCollectionLayoutFactory[G, V, E])
                            (implicit sparkSession: SparkSession): GradoopSparkConfig[G, V, E, LG, GC] = {
    val config = new GradoopSparkConfig[G, V, E, LG, GC](null, null)

    config.setLogicalGraphFactory(new LogicalGraphFactory(logicalGraphLayoutFactory, config))
    config.setGraphCollectionFactory(new GraphCollectionFactory(graphCollectionLayoutFactory, config))

    config
  }
}