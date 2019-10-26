package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.{Encoder, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, ElementFactoryProvider, GraphHead, GraphHeadFactory, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig

abstract class BaseGraphFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]](config: GradoopSparkConfig[G, V, E, LG, GC])
  extends ElementFactoryProvider[G, V, E] {
  implicit val encoderG: Encoder[G] = config.getGraphHeadEncoder
  implicit val encoderV: Encoder[V] = config.getVertexEncoder
  implicit val encoderE: Encoder[E] = config.getEdgeEncoder
  implicit val session: SparkSession = config.getSparkSession
}
