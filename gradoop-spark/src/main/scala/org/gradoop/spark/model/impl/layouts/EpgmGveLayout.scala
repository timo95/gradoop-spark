package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, GveLayout, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}

class EpgmGveLayout(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E])
  extends GveLayout[G, V, E](graphHeads, vertices, edges)

object EpgmGveLayout extends LogicalGraphLayoutFactory[G, V, E] with GraphCollectionLayoutFactory[G, V, E] {

  override def getGraphHeadFactory: GraphHeadFactory[G] = EpgmGraphHead

  override def getVertexFactory: VertexFactory[V] = EpgmVertex

  override def getEdgeFactory: EdgeFactory[E] = EpgmEdge

  /** Creates a Epgm Gve layout from the given Datasets.
   *
   * @param graphHeads EPGMGraphHead Dataset
   * @param vertices   EPGMVertex Dataset
   * @param edges      EPGMEdge Dataset
   * @return Epgm GVE layout
   */
  def apply(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GveLayout[G, V, E] = new EpgmGveLayout(graphHeads, vertices, edges)
}