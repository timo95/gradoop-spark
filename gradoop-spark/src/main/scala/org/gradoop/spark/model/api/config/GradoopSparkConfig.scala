package org.gradoop.spark.model.api.config

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}

abstract class GradoopSparkConfig[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection](implicit sparkSession: SparkSession) {

  /** Creates instances of {@link LogicalGraph} */
  var logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC]

  /** Creates instances of {@link GraphCollection} */
  var graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC]


  def getLogicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC] = logicalGraphFactory
  def getGraphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC] = graphCollectionFactory

  def getSparkSession: SparkSession = sparkSession

  def getGraphHeadEncoder: Encoder[G] = Encoders.kryo[G]
  def getVertexEncoder: Encoder[V] = Encoders.kryo[V]
  def getEdgeEncoder: Encoder[E] = Encoders.kryo[E]
}
