package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.gve.{EdgeFactory, ElementFactoryProvider, GraphHeadFactory, VertexFactory}
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{GveBaseLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.GveLayoutType

/** Creates a logical graph with a specific layout. */
class LogicalGraphFactory[L <: GveLayoutType](val layoutFactory: GveBaseLayoutFactory[L] with LogicalGraphLayoutFactory[L], config: GradoopSparkConfig[L])
  extends BaseGraphFactory[L](layoutFactory, config) {

  /** Creates a logical graph from the given vertices and edges.
   *
   * The method creates a new graph head element and assigns the vertices and edges to that graph.
   *
   * @param vertices Vertex Dataset
   * @param edges    Edge Dataset
   * @return Logical graph
   */
  def create(vertices: Dataset[L#V], edges: Dataset[L#E]): LogicalGraph[L] = {
    val id = GradoopId.get
    val graphHeads = session.createDataset[L#G](Seq(graphHeadFactory(id)))

    // TODO add/set id to each element

   init(graphHeads, vertices, edges)
  }

  /** Creates a logical graph from the given graph head, vertices and edges.
   *
   * The method assumes that the given vertices and edges are already assigned to the given graph head.
   *
   * @param graphHead 1-element GraphHead Dataset
   * @param vertices  Vertex Dataset
   * @param edges     Edge Dataset
   * @return Logical graph
   */
  def init(graphHead: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): LogicalGraph[L] = {
    layoutFactory.createLogicalGraph(layoutFactory(graphHead, vertices, edges), config)
    // TODO limit to graphHead.first?
  }

  /** Creates a logical graph from the given single graph head, vertex and edge collections.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph
   */
  def init(graphHead: L#G, vertices: Iterable[L#V], edges: Iterable[L#E]): LogicalGraph[L] = {
    val graphHeadDS: Dataset[L#G] = session.createDataset[L#G](Seq(graphHead))
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  /**
   * Creates a logical graph from the given vertex and edge collections. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param vertices Vertex collection
   * @param edges    Edge collection
   * @return Logical graph
   */
  def init(vertices: Iterable[L#V], edges: Iterable[L#E]): LogicalGraph[L] = {
    val graphHead = graphHeadFactory(GradoopId.get)
    init(graphHead, vertices, edges)
  }

  /** Creates an empty graph.
   *
   * @return empty graph
   */
  def empty: LogicalGraph[L] = {
    val graphHeads: Dataset[L#G] = session.emptyDataset[L#G]
    val vertices: Dataset[L#V] = session.emptyDataset[L#V]
    val edges: Dataset[L#E] = session.emptyDataset[L#E]

    init(graphHeads, vertices, edges)
  }
}
