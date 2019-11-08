package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, ElementFactoryProvider, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.BaseLayoutFactory

abstract class BaseGraphFactory[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(layoutFactory: BaseLayoutFactory[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends ElementFactoryProvider[G, V, E] {

  implicit val graphHeadEncoder: Encoder[G] = layoutFactory.graphHeadEncoder
  implicit val vertexEncoder: Encoder[V] = layoutFactory.vertexEncoder
  implicit val edgeEncoder: Encoder[E] = layoutFactory.edgeEncoder

  implicit val session: SparkSession = config.sparkSession

  def createDataset[T](iterable: Iterable[T])(implicit encoder: Encoder[T]): Dataset[T] =
    session.createDataset(iterable.toSeq)
}
