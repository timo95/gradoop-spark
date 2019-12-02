package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.gve.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{BaseGraph, BaseGraphFactory}
import org.gradoop.spark.model.api.layouts.GveBaseLayoutFactory

class EpgmGveLayoutFactory[G <: BaseGraph[L]](var config: GradoopSparkConfig[L], graphFactory: BaseGraphFactory[L, G])
                                             (implicit session: SparkSession) extends GveBaseLayoutFactory[L, G] {

  override implicit def sparkSession: SparkSession = session

  override def graphHeadEncoder: Encoder[L#G] = EpgmGraphHead.encoder

  override def vertexEncoder: Encoder[L#V] = EpgmVertex.encoder

  override def edgeEncoder: Encoder[L#E] = EpgmEdge.encoder

  override def graphHeadFactory: GraphHeadFactory[L#G] = EpgmGraphHead

  override def vertexFactory: VertexFactory[L#V] = EpgmVertex

  override def edgeFactory: EdgeFactory[L#E] = EpgmEdge

  override def init(graphHead: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): G = {
    graphFactory.createGraph(new EpgmGveLayout(graphHead, vertices, edges), config)
  }
}
