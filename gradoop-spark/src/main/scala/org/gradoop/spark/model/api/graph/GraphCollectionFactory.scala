package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, EdgeFactory, GraphHead, GraphHeadFactory, Vertex, VertexFactory}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.GraphCollectionLayoutFactory

/** Creates a graph collection with a specific layout.
 *
 * @tparam G
 * @tparam V
 * @tparam E
 * @tparam LG
 * @tparam GC
 */
abstract class GraphCollectionFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(layoutFactory: GraphCollectionLayoutFactory[G, V, E], config: GradoopSparkConfig[G, V, E, LG, GC])
  extends BaseGraphFactory[G, V, E, LG, GC](layoutFactory, config) {

  override def getGraphHeadFactory: GraphHeadFactory[G] = layoutFactory.getGraphHeadFactory

  override def getVertexFactory: VertexFactory[V] = layoutFactory.getVertexFactory

  override def getEdgeFactory: EdgeFactory[E] = layoutFactory.getEdgeFactory

  /** Creates a graph collection layout from the given datasets.
   *
   * @param graphHeads GraphHead Dataset
   * @param vertices   Vertex Dataset
   * @param edges      Edge Dataset
   * @return Graph collection
   */
  def init(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E]): GC

  /** Creates a graph collection from the given collections.
   *
   * @param graphHeads Graph Head collection
   * @param vertices   Vertex collection
   * @param edges      Edge collection
   * @return Graph collection
   */
  def init(graphHeads: Iterable[G], vertices: Iterable[V], edges: Iterable[E]): GC = {
    val graphHeadDS: Dataset[G] = createDataset[G](graphHeads)
    val vertexDS: Dataset[V] = createDataset[V](vertices)
    val edgeDS: Dataset[E] = createDataset[E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  /** Creates a graph collection from multiple given logical graphs.
   *
   * @param logicalGraphs input graphs
   * @return graph collection
   */
  def init(logicalGraphs: LG*): GC = {
    val graphHeads: Dataset[G] = session.emptyDataset[G]
    val vertices: Dataset[V] = session.emptyDataset[V]
    val edges: Dataset[E] = session.emptyDataset[E]

    for (logicalGraph <- logicalGraphs) {
      graphHeads.union(logicalGraph.getGraphHead)
      vertices.union(logicalGraph.getVertices)
      edges.union(logicalGraph.getEdges)
    }

    init(graphHeads, vertices, edges)
  }

  /** Creates an empty graph collection.
   *
   * @return empty graph collection
   */
  def empty: GC = {
    var graphHeads: Dataset[G] = session.emptyDataset[G]
    var vertices: Dataset[V] = session.emptyDataset[V]
    var edges: Dataset[E] = session.emptyDataset[E]

    init(graphHeads, vertices, edges)
  }
}
