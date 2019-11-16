package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts._

class EpgmGveLayout(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E])
  extends GveLayout[L](graphHeads, vertices, edges)

object EpgmGveLayout extends EpgmBaseLayoutFactory
  with LogicalGraphLayoutFactory[L]
  with GraphCollectionLayoutFactory[L] {

  override def createLogicalGraph(layout: LogicalGraphLayout[L], config: GradoopSparkConfig[L]):
  LogicalGraph[L] = new LogicalGraph[L](layout, config)

  override def createGraphCollection(layout: GraphCollectionLayout[L], config:
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