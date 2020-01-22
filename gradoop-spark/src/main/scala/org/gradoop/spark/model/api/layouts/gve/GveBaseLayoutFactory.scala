package org.gradoop.spark.model.api.layouts.gve

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.components.ComponentTypes
import org.gradoop.common.model.api.gve.GveElementFactoryProvider
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.Implicits

trait GveBaseLayoutFactory[L <: Gve[L], BG <: BaseGraph[L]] extends LogicalGraphLayoutFactory[L]
  with GraphCollectionLayoutFactory[L]
  with GveElementFactoryProvider[L#G, L#V, L#E] {

  object Implicits extends Implicits with ComponentTypes {
    // Encoder
    implicit def implicitGraphHeadEncoder: Encoder[L#G] = graphHeadEncoder
    implicit def impliticVertexEncoder: Encoder[L#V] = vertexEncoder
    implicit def implicitEdgeEncoder: Encoder[L#E] = edgeEncoder
  }

  implicit def graphHeadEncoder: Encoder[L#G]

  implicit def vertexEncoder: Encoder[L#V]

  implicit def edgeEncoder: Encoder[L#E]

  def init(graphHead: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): BG

  def init(graphHead: L#G, vertices: Iterable[L#V], edges: Iterable[L#E]): BG = {
    val graphHeadDS: Dataset[L#G] = sparkSession.createDataset[L#G](Seq(graphHead))
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

  def init(vertices: Iterable[L#V], edges: Iterable[L#E]): BG = {
    val graphHead = graphHeadFactory(GradoopId.get)
    init(graphHead, vertices, edges)
  }

  def create(vertices: Iterable[L#V], edges: Iterable[L#E]): BG = {
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    create(vertexDS, edgeDS)
  }

  /** This creates a new graph head/id and adds it to each element. */
  def create(vertices: Dataset[L#V], edges: Dataset[L#E]): BG = {
    val id = GradoopId.get
    val graphHeads = sparkSession.createDataset[L#G](Seq(graphHeadFactory(id)))

    val tfV = TransformationFunctions.addGraphId[L#V](id)
    val tfE = TransformationFunctions.addGraphId[L#E](id)
    init(graphHeads, tfV(vertices), tfE(edges))
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
