package org.gradoop.spark.util

import java.io.InputStream

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.{AsciiGraphLoader, GradoopConstants}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}


class SparkAsciiGraphLoader[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(config: GradoopSparkConfig[G, V, E, LG, GC], loader: AsciiGraphLoader[G, V, E]) {

  /** Appends the given ASCII GDL String to the database.
   *
   * Variables previously used can be reused as their refer to the same objects.
   *
   * @param asciiGraph GDL string (must not be { @code null})
   */
  def appendToDatabaseFromString(asciiGraph: String): Unit = {
    if (asciiGraph == null) throw new IllegalArgumentException("AsciiGraph must not be null")
    loader.appendFromString(asciiGraph)
  }

  /** Returns a logical graph containing the complete vertex and edge space of the database.
   * This is equivalent to {@link #logicalGraph(boolean) logicalGraph(true)}.
   *
   * @return logical graph of vertex and edge space
   */
  def logicalGraph: LG = logicalGraph(true)

  /** Returns a logical graph containing the complete vertex and edge space of the database.
   *
   * @param withGraphContainment true, if vertices and edges shall be updated to be contained in the logical graph
   *                             representing the database
   * @return logical graph of vertex and edge space
   */
  def logicalGraph(withGraphContainment: Boolean): LG = {
    val factory = config.logicalGraphFactory
    if (withGraphContainment) factory.init(vertices, edges)
      // TODO .transformGraphHead(new RenameLabel[G](GradoopConstants.DEFAULT_GRAPH_LABEL, GradoopConstants.DB_GRAPH_LABEL))
    else {
      val graphHead = factory.graphHeadFactory.create(GradoopConstants.DB_GRAPH_LABEL)
      factory.init(graphHead, vertices, edges)
    }
  }

  /** Builds a {@link LogicalGraph} from the graph referenced by the given graph variable.
   *
   * @param variable graph variable used in GDL script
   * @return LogicalGraph
   */
  def logicalGraphByVariable(variable: String): LG = {
    val graphHead = graphHeadByVariable(variable)

    graphHead match {
      case Some(g) => config.logicalGraphFactory.init(g,
        verticesByGraphVariables(variable),
        edgesByGraphVariables(variable))
      case None => config.logicalGraphFactory.empty
    }
  }

  /** Returns a collection of all logical graph contained in the database.
   *
   * @return collection of all logical graphs
   */
  def graphCollection: GC = {
    config.graphCollectionFactory.init(graphHeads,
      vertices.filter(v => v.graphCount > 0),
      edges.filter(e => e.graphCount > 0))
  }

  /** Builds a {@link GraphCollection} from the graph referenced by the given graph variables.
   *
   * @param variables graph variables used in GDL script
   * @return GraphCollection
   */
  def graphCollectionByVariables(variables: String*): GC = {
    val graphHeads = graphHeadsByVariables(variables: _*)
    val vertices = verticesByGraphVariables(variables: _*)
    val edges = edgesByGraphVariables(variables: _*)
    config.graphCollectionFactory.init(graphHeads, vertices, edges)
  }

  /** Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  def graphHeads: Iterable[G] = loader.graphHeads

  /** Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or { @code null} if graph is not cached
   */
  def graphHeadByVariable(variable: String): Option[G] = loader.graphHeadsByVariable(variable)

  /** Returns the graph heads assigned to the specified variables.
   *
   * @param variables variables used in the GDL script
   * @return graphHeads assigned to the variables
   */
  def graphHeadsByVariables(variables: String*): Set[G] = loader.graphHeadsByVariables(variables: _*)

  /** Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  def vertices: Iterable[V] = loader.vertices

  /** Returns all vertices that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  def verticesByGraphVariables(variables: String*): Set[V] = loader.verticesByGraphVariables(variables: _*)

  /** Returns the vertex which is identified by the given variable.
   *
   * If the variable cannot be found, the method returns {@code None}.
   *
   * @param variable vertex variable
   * @return vertex or { @code null} if variable is not used
   */
  def vertexByVariable(variable: String): Option[V] = loader.verticesByVariable(variable)

  /** Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  def edges: Iterable[E] = loader.edges

  /** Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  def edgesByGraphVariables(variables: String*): Iterable[E] = loader.edgesByGraphVariables(variables: _*)

  /** Returns the edge which is identified by the given variable.
   *
   * If the variable cannot be found, the method returns {@code None}.
   *
   * @param variable edge variable
   * @return edge or { @code null} if variable is not used
   */
  def edgeByVariable(variable: String): Option[E] = loader.edgeByVariable(variable)
}

object SparkAsciiGraphLoader {

  /** Initializes the database from the given ASCII GDL string.
   *
   * @param asciiGraphs GDL string
   */
  def fromString[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
  (config: GradoopSparkConfig[G, V, E, LG, GC], asciiGraphs: String): SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    val loader = AsciiGraphLoader.fromString(config.logicalGraphFactory, asciiGraphs)
    new SparkAsciiGraphLoader(config, loader)
  }

  /** Initializes the database from the given GDL file.
   *
   * @param fileName GDL file name
   */
  def fromFile[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
  (config: GradoopSparkConfig[G, V, E, LG, GC], fileName: String): SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    val loader = AsciiGraphLoader.fromFile(config.logicalGraphFactory, fileName)
    new SparkAsciiGraphLoader(config, loader)
  }

  /** Initializes the database from the given ASCII GDL stream.
   *
   * @param stream GDL stream
   */
  def fromStream[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
  (config: GradoopSparkConfig[G, V, E, LG, GC], stream: InputStream): SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    val loader = AsciiGraphLoader.fromStream(config.logicalGraphFactory, stream)
    new SparkAsciiGraphLoader(config, loader)
  }

}