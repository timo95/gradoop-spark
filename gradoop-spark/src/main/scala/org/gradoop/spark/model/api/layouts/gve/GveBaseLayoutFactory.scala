package org.gradoop.spark.model.api.layouts.gve

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.model.api.gve.GveElementFactoryProvider
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.transformation.TransformationFunctions

trait GveBaseLayoutFactory[L <: Gve[L], +BG <: BaseGraph[L]] extends LogicalGraphLayoutFactory[L]
  with GraphCollectionLayoutFactory[L]
  with GveElementFactoryProvider[L#G, L#V, L#E] {

  object Implicits {
    // Encoder
    implicit def implicitGveGraphHeadEncoder: Encoder[L#G] = graphHeadEncoder
    implicit def impliticGveVertexEncoder: Encoder[L#V] = vertexEncoder
    implicit def implicitGveEdgeEncoder: Encoder[L#E] = edgeEncoder
  }

  implicit def graphHeadEncoder: Encoder[L#G]

  implicit def vertexEncoder: Encoder[L#V]

  implicit def edgeEncoder: Encoder[L#E]

  def init(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): BG

  def init(graphHeads: L#G, vertices: Iterable[L#V], edges: Iterable[L#E]): BG = {
    val graphHeadDS: Dataset[L#G] = sparkSession.createDataset[L#G](Seq(graphHeads))
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  def init(graphHeads: Iterable[L#G], vertices: Iterable[L#V], edges: Iterable[L#E]): BG = {
    val graphHeadDS: Dataset[L#G] = createDataset[L#G](graphHeads)
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  /** This creates a new graph head and adds its id to each element. */
  def create(vertices: Iterable[L#V], edges: Iterable[L#E]): BG = {
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    create(vertexDS, edgeDS)
  }

  /** This creates a new graph head and adds its id to each element. */
  def create(vertices: Dataset[L#V], edges: Dataset[L#E]): BG = {
    val graphHead = graphHeadFactory.create
    val graphHeads = sparkSession.createDataset[L#G](Seq(graphHead))

    val addToV = TransformationFunctions.addGraphId[L#V](graphHead.id)
    val addToE = TransformationFunctions.addGraphId[L#E](graphHead.id)
    init(graphHeads, addToV(vertices), addToE(edges))
  }

  def init(gveLayout: GveLayout[L], gveLayouts: GveLayout[L]*): BG = {
    val graphHeads = gveLayout.graphHead
    val vertices = gveLayout.vertices
    val edges = gveLayout.edges

    for (gveLayout: GveLayout[L] <- gveLayouts) {
      graphHeads.union(gveLayout.graphHead)
      vertices.union(gveLayout.vertices)
      edges.union(gveLayout.edges)
    }

    init(graphHeads, vertices, edges)
  }

  /** Creates an empty graph.
   *
   * @return empty graph
   */
  def empty: BG = {
    val graphHeads: Dataset[L#G] = sparkSession.emptyDataset[L#G]
    val vertices: Dataset[L#V] = sparkSession.emptyDataset[L#V]
    val edges: Dataset[L#E] = sparkSession.emptyDataset[L#E]

    init(graphHeads, vertices, edges)
  }
}
