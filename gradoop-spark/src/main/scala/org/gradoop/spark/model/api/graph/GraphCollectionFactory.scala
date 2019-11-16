package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements._
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.GraphCollectionLayoutFactory
import org.gradoop.spark.model.impl.types.GveGraphLayout

/** Creates a graph collection with a specific layout. */
class GraphCollectionFactory[L <: GveGraphLayout]
(layoutFactory: GraphCollectionLayoutFactory[L],
 config: GradoopSparkConfig[L])
  extends BaseGraphFactory[L](layoutFactory, config) {

  override def graphHeadFactory: GraphHeadFactory[L#G] = layoutFactory.graphHeadFactory

  override def vertexFactory: VertexFactory[L#V] = layoutFactory.vertexFactory

  override def edgeFactory: EdgeFactory[L#E] = layoutFactory.edgeFactory

  /** Creates a graph collection layout from the given datasets.
   *
   * @param graphHeads GraphHead Dataset
   * @param vertices   Vertex Dataset
   * @param edges      Edge Dataset
   * @return Graph collection
   */
  def init(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): GraphCollection[L] = {
    layoutFactory.createGraphCollection(layoutFactory(graphHeads, vertices, edges), config)
  }

  /** Creates a graph collection from the given collections.
   *
   * @param graphHeads Graph Head collection
   * @param vertices   Vertex collection
   * @param edges      Edge collection
   * @return Graph collection
   */
  def init(graphHeads: Iterable[L#G], vertices: Iterable[L#V], edges: Iterable[L#E]): GraphCollection[L] = {
    val graphHeadDS: Dataset[L#G] = createDataset[L#G](graphHeads)
    val vertexDS: Dataset[L#V] = createDataset[L#V](vertices)
    val edgeDS: Dataset[L#E] = createDataset[L#E](edges)

    init(graphHeadDS, vertexDS, edgeDS)
  }

  /** Creates a graph collection from multiple given logical graphs.
   *
   * @param logicalGraphs input graphs
   * @return graph collection
   */
  def init(logicalGraph: LogicalGraph[L], logicalGraphs: LogicalGraph[L]*): GraphCollection[L] = {
    val graphHeads = logicalGraph.graphHead
    val vertices = logicalGraph.vertices
    val edges = logicalGraph.edges

    for (logicalGraph: LogicalGraph[L] <- logicalGraphs) {
      graphHeads.union(logicalGraph.graphHead)
      vertices.union(logicalGraph.vertices)
      edges.union(logicalGraph.edges)
    }

    init(graphHeads, vertices, edges)
  }

  /** Creates an empty graph collection.
   *
   * @return empty graph collection
   */
  def empty: GraphCollection[L] = {
    var graphHeads: Dataset[L#G] = session.emptyDataset[L#G]
    var vertices: Dataset[L#V] = session.emptyDataset[L#V]
    var edges: Dataset[L#E] = session.emptyDataset[L#E]

    init(graphHeads, vertices, edges)
  }
}
