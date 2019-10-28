package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements._
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.LogicalGraphLayoutFactory

/** Creates a logical graph with a specific layout.
 *
 * @tparam G
 * @tparam V
 * @tparam E
 * @tparam LG
 * @tparam GC
 */
class LogicalGraphFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(layoutFactory: LogicalGraphLayoutFactory[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends BaseGraphFactory[G, V, E, LG, GC](config) {

  override def getGraphHeadFactory: GraphHeadFactory[G] = layoutFactory.getGraphHeadFactory

  override def getVertexFactory: VertexFactory[V] = layoutFactory.getVertexFactory

  override def getEdgeFactory: EdgeFactory[E] = layoutFactory.getEdgeFactory

  /** Creates a logical graph from the given vertices and edges.
   *
   * The method creates a new graph head element and assigns the vertices and edges to that graph.
   *
   * @param vertices Vertex Dataset
   * @param edges    Edge Dataset
   * @return Logical graph
   */
  def create(vertices: Dataset[V], edges: Dataset[E]): LG = {
    val id = GradoopId.get
    val graphHeads = session.createDataset[G](Seq(getGraphHeadFactory(id)))

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
  def init(graphHead: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): LG = {
    val layout = layoutFactory(graphHead, vertices, edges)
    new LG(layout, config) // TODO geht das wirklich?????
  }

  /** Creates a logical graph from the given single graph head, vertex and edge collections.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph
   */
  def init(graphHead: G, vertices: Iterable[V], edges: Iterable[E]): LG = {
    val graphHeadDS: Dataset[G] = session.createDataset[G](Seq(graphHead))
    val vertexDS: Dataset[V] = createDataset[V](vertices)
    val edgeDS: Dataset[E] = createDataset[E](edges)

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
  def init(vertices: Iterable[V], edges: Iterable[E]): LG = {
    val vertexDS: Dataset[V] = createDataset[V](vertices)
    val edgeDS: Dataset[E] = createDataset[E](edges)

    init(vertices, edges)
  }

  /** Creates an empty graph.
   *
   * @return empty graph
   */
  def empty: LG = {
    val graphHeads: Dataset[G] = session.emptyDataset[G]
    val vertices: Dataset[V] = session.emptyDataset[V]
    val edges: Dataset[E] = session.emptyDataset[E]

    init(graphHeads, vertices, edges)
  }
}
