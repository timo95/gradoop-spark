package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.model.api.gve.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts._

class EpgmGveLayout(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E])
  extends GveLayout[L](graphHeads, vertices, edges)

object EpgmGveLayout extends GveBaseLayoutFactory[L] {

  override def graphHeadEncoder: Encoder[L#G] = EpgmGraphHead.encoder

  override def vertexEncoder: Encoder[L#V] = EpgmVertex.encoder

  override def edgeEncoder: Encoder[L#E] = EpgmEdge.encoder

  override def graphHeadFactory: GraphHeadFactory[L#G] = EpgmGraphHead

  override def vertexFactory: VertexFactory[L#V] = EpgmVertex

  override def edgeFactory: EdgeFactory[L#E] = EpgmEdge

  override def createLogicalGraph(layout: GveLayout[L] with LogicalGraphLayout[L], config: GradoopSparkConfig[L]):
  LogicalGraph[L] = new LogicalGraph[L](layout, config)

  override def createGraphCollection(layout: GveLayout[L] with GraphCollectionLayout[L], config:
  GradoopSparkConfig[L]): GraphCollection[L] = new GraphCollection[L](layout, config)

  /** Creates a Epgm Gve layout from the given Datasets.
   *
   * @param graphHeads EPGMGraphHead Dataset
   * @param vertices   EPGMVertex Dataset
   * @param edges      EPGMEdge Dataset
   * @return Epgm GVE layout
   */
  def apply(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): GveLayout[L] =
    new EpgmGveLayout(graphHeads, vertices, edges)
}