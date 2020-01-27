package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.{Encoder, SparkSession}
import org.gradoop.common.model.api.tfl.{TflEdgeFactory, TflGraphHeadFactory, TflPropertiesFactory, TflVertexFactory}
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.tfl.TflBaseLayoutFactory

abstract class EpgmTflLayoutFactory[BG <: BaseGraph[L]](implicit session: SparkSession)
  extends TflBaseLayoutFactory[L, BG] {

  override implicit def sparkSession: SparkSession = session

  override def graphHeadEncoder: Encoder[L#G] = EpgmTflGraphHead.encoder

  override def vertexEncoder: Encoder[L#V] = EpgmTflVertex.encoder

  override def edgeEncoder: Encoder[L#E] = EpgmTflEdge.encoder

  override def propertiesEncoder: Encoder[L#P] = EpgmTflProperties.encoder

  override def graphHeadFactory: TflGraphHeadFactory[L#G] = EpgmTflGraphHead

  override def vertexFactory: TflVertexFactory[L#V] = EpgmTflVertex

  override def edgeFactory: TflEdgeFactory[L#E] = EpgmTflEdge

  override def propertiesFactory: TflPropertiesFactory[L#P] = EpgmTflProperties
}
