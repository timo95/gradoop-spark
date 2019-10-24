package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

abstract class GveLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection]
  extends LogicalGraphLayoutFactory[G, V, E, LG, GC] with GraphCollectionLayoutFactory[G, V, E, LG, GC] {

  /**
   * Creates a collection layout from the given Datasets.
   *
   * @param graphHeads EPGMGraphHead Dataset
   * @param vertices   EPGMVertex Dataset
   * @param edges      EPGMEdge Dataset
   * @return GVE layout
   */
  def create(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GveLayout[G, V, E] = {
    new GveLayout(graphHeads, vertices, edges)
  }

  /**
   * Creates a collection layout from the given Datasets indexed by label.
   *
   * @param graphHeads Mapping from label to graph head Dataset
   * @param vertices   Mapping from label to vertex Dataset
   * @param edges      Mapping from label to edge Dataset
   * @return GVE layout
   */
  def create(graphHeads: Map[String, Dataset[G]], vertices: Map[String, Dataset[V]], edges: Map[String, Dataset[E]]): GveLayout[G, V, E] = {
    // TODO
  }
}
