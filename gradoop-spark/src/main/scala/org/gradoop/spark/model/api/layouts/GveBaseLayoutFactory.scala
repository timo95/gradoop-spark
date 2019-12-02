package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.model.api.ComponentTypes
import org.gradoop.common.model.api.gve.ElementFactoryProvider
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.impl.types.GveLayoutType
import org.gradoop.spark.util.Implicits

trait GveBaseLayoutFactory[L <: GveLayoutType, G <: BaseGraph[L]] extends LogicalGraphLayoutFactory[L] with GraphCollectionLayoutFactory[L]
  with ElementFactoryProvider[L#G, L#V, L#E] {

  object implicits extends Implicits with ComponentTypes {
    // Encoder
    implicit def implicitGraphHeadEncoder: Encoder[L#G] = graphHeadEncoder
    implicit def impliticVertexEncoder: Encoder[L#V] = vertexEncoder
    implicit def implicitEdgeEncoder: Encoder[L#E] = edgeEncoder
  }

  implicit def graphHeadEncoder: Encoder[L#G]

  implicit def vertexEncoder: Encoder[L#V]

  implicit def edgeEncoder: Encoder[L#E]

  def init(graphHead: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): G

  def init(graphHead: L#G, vertices: Iterable[L#V], edges: Iterable[L#E]): G = {
    val graphHeadDS: Dataset[L#G] = sparkSession.createDataset[L#G](Seq(graphHead))
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  def init(graphHeads: Iterable[L#G], vertices: Iterable[L#V], edges: Iterable[L#E]): G = {
    val graphHeadDS: Dataset[L#G] = createDataset[L#G](graphHeads)
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  def init(vertices: Iterable[L#V], edges: Iterable[L#E]): G = {
    val graphHead = graphHeadFactory(GradoopId.get)
    init(graphHead, vertices, edges)
  }

  def create(vertices: Dataset[L#V], edges: Dataset[L#E]): G = {
    val id = GradoopId.get
    val graphHeads = sparkSession.createDataset[L#G](Seq(graphHeadFactory(id)))

    // TODO add/set id to each element

    init(graphHeads, vertices, edges)
  }

  def init(gveLayout: GveLayout[L], gveLayouts: GveLayout[L]*): G = {
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
  def empty: G = {
    val graphHeads: Dataset[L#G] = sparkSession.emptyDataset[L#G]
    val vertices: Dataset[L#V] = sparkSession.emptyDataset[L#V]
    val edges: Dataset[L#E] = sparkSession.emptyDataset[L#E]

    init(graphHeads, vertices, edges)
  }
}
