package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.gradoop.common.model.api.elements.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayout, GraphCollectionLayoutFactory, GveLayout, LogicalGraphLayout, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}
import org.gradoop.spark.model.impl.graph.{EpgmGraphCollection, EpgmLogicalGraph}

class EpgmGveLayout(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E])
  extends GveLayout[G, V, E](graphHeads, vertices, edges)

object EpgmGveLayout extends LogicalGraphLayoutFactory[G, V, E, LG, GC] with GraphCollectionLayoutFactory[G, V, E, LG, GC] {

  override def getGraphHeadEncoder: Encoder[G] = Encoders.kryo[G]

  override def getVertexEncoder: Encoder[V] = Encoders.kryo[V]

  override def getEdgeEncoder: Encoder[E] = Encoders.kryo[E]

  override def getGraphHeadFactory: GraphHeadFactory[G] = EpgmGraphHead

  override def getVertexFactory: VertexFactory[V] = EpgmVertex

  override def getEdgeFactory: EdgeFactory[E] = EpgmEdge

  override def createLogicalGraph(layout: LogicalGraphLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC]): LG = {
    new EpgmLogicalGraph(layout, config)
  }

  override def createGraphCollection(layout: GraphCollectionLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC]): GC = {
    new EpgmGraphCollection(layout, config)
  }

  /** Creates a Epgm Gve layout from the given Datasets.
   *
   * @param graphHeads EPGMGraphHead Dataset
   * @param vertices   EPGMVertex Dataset
   * @param edges      EPGMEdge Dataset
   * @return Epgm GVE layout
   */
  def apply(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GveLayout[G, V, E] = new EpgmGveLayout(graphHeads, vertices, edges)
}