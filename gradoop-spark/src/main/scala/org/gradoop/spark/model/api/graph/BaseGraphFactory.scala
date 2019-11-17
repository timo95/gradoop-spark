package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.ElementFactoryProvider
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.BaseLayoutFactory
import org.gradoop.spark.model.impl.types.GveLayoutType

abstract class BaseGraphFactory[L <: GveLayoutType]
(layoutFactory: BaseLayoutFactory[L], config: GradoopSparkConfig[L]) extends ElementFactoryProvider[L#G, L#V, L#E] {

  implicit val graphHeadEncoder: Encoder[L#G] = layoutFactory.graphHeadEncoder
  implicit val vertexEncoder: Encoder[L#V] = layoutFactory.vertexEncoder
  implicit val edgeEncoder: Encoder[L#E] = layoutFactory.edgeEncoder

  implicit val session: SparkSession = config.sparkSession

  def createDataset[T](iterable: Iterable[T])(implicit encoder: Encoder[T]): Dataset[T] =
    session.createDataset(iterable.toSeq)
}
