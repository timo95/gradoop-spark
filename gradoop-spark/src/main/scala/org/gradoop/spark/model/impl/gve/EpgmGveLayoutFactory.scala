package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.{Encoder, SparkSession}
import org.gradoop.common.model.api.gve.{GveEdgeFactory, GveGraphHeadFactoryGve, GveVertexFactory}
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.gve.GveBaseLayoutFactory

abstract class EpgmGveLayoutFactory[BG <: BaseGraph[L]](implicit session: SparkSession)
  extends GveBaseLayoutFactory[L, BG] {

  override implicit def sparkSession: SparkSession = session

  override def graphHeadEncoder: Encoder[L#G] = EpgmGveGraphHead.encoder

  override def vertexEncoder: Encoder[L#V] = EpgmGveVertex.encoder

  override def edgeEncoder: Encoder[L#E] = EpgmGveEdgeFactory.encoder

  override def graphHeadFactory: GveGraphHeadFactoryGve[L#G] = EpgmGveGraphHead

  override def vertexFactory: GveVertexFactory[L#V] = EpgmGveVertex

  override def edgeFactory: GveEdgeFactory[L#E] = EpgmGveEdgeFactory
}
