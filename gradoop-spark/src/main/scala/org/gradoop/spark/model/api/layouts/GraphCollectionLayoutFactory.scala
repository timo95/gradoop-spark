package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait GraphCollectionLayoutFactory[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]] extends BaseLayoutFactory[G, V, E] {
  def apply(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GraphCollectionLayout[G, V, E]

  def createGraphCollection(layout: GraphCollectionLayout[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC]): GC
}
