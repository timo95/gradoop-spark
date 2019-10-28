package org.gradoop.spark.util

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.{AsciiGraphLoader, GradoopConstants}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}


class SparkAsciiGraphLoader[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
(config: GradoopSparkConfig[G, V, E, LG, GC], loader: AsciiGraphLoader[G, V, E]) {

  /**
   * Appends the given ASCII GDL String to the database.
   *
   * Variables previously used can be reused as their refer to the same objects.
   *
   * @param asciiGraph GDL string (must not be { @code null})
   */
  def appendToDatabaseFromString(asciiGraph: String): Unit = {
    if (asciiGraph == null) throw new IllegalArgumentException("AsciiGraph must not be null")
    loader.appendFromString(asciiGraph)
  }

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * the database.
   * This is equivalent to {@link #getLogicalGraph(boolean) getLogicalGraph(true)}.
   *
   * @return logical graph of vertex and edge space
   */
  def getLogicalGraph: LG = getLogicalGraph(true)

  /** Returns a logical graph containing the complete vertex and edge space of the database.
   *
   * @param withGraphContainment true, if vertices and edges shall be updated to be contained in the logical graph
   *                             representing the database
   * @return logical graph of vertex and edge space
   */
  def getLogicalGraph(withGraphContainment: Boolean): LG = {
    val factory = config.getLogicalGraphFactory
    if (withGraphContainment) factory.init(getVertices, getEdges)
      // TODO .transformGraphHead(new RenameLabel[G](GradoopConstants.DEFAULT_GRAPH_LABEL, GradoopConstants.DB_GRAPH_LABEL))
    else {
      val graphHead = factory.getGraphHeadFactory.create(Array(GradoopConstants.DB_GRAPH_LABEL))
      factory.init(graphHead, getVertices, getEdges)
    }
  }

  /** Builds a {@link LogicalGraph} from the graph referenced by the given graph variable.
   *
   * @param variable graph variable used in GDL script
   * @return LogicalGraph
   */
  def getLogicalGraphByVariable(variable: String): LG = {
    val graphHead = getGraphHeadByVariable(variable)
    val vertices = getVerticesByGraphVariables(variable)
    val edges = getEdgesByGraphVariables(variable)

    graphHead match {
      case Some(g) => config.getLogicalGraphFactory.init(g, vertices, edges)
      case None => config.getLogicalGraphFactory.empty
    }
  }

  /**
   * Returns a collection of all logical graph contained in the database.
   *
   * @return collection of all logical graphs
   */
  def getGraphCollection: GC = {
    val vertices = getVertices.filter(v => v.getGraphCount > 0)
    val edges = getEdges.filter(e => e.getGraphCount > 0)
    config.getGraphCollectionFactory.init(getGraphHeads, vertices, edges)
  }

  /**
   * Builds a {@link GraphCollection} from the graph referenced by the given graph variables.
   *
   * @param variables graph variables used in GDL script
   * @return GraphCollection
   */
  def getGraphCollectionByVariables(variables: String*): GC = {
    val graphHeads = getGraphHeadsByVariables(variables: _*)
    val vertices = getVerticesByGraphVariables(variables: _*)
    val edges = getEdgesByGraphVariables(variables: _*)
    config.getGraphCollectionFactory.init(graphHeads, vertices, edges)
  }

  /**
   * Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  def getGraphHeads: Iterable[G] = loader.getGraphHeads

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or { @code null} if graph is not cached
   */
  def getGraphHeadByVariable(variable: String): Option[G] = loader.getGraphHeadByVariable(variable)

  /**
   * Returns the graph heads assigned to the specified variables.
   *
   * @param variables variables used in the GDL script
   * @return graphHeads assigned to the variables
   */
  def getGraphHeadsByVariables(variables: String*): Set[G] = loader.getGraphHeadsByVariables(variables: _*)

  /**
   * Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  def getVertices: Iterable[V] = loader.getVertices

  /**
   * Returns all vertices that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  def getVerticesByGraphVariables(variables: String*): Set[V] = loader.getVerticesByGraphVariables(variables: _*)

  /**
   * Returns the vertex which is identified by the given variable. If the variable cannot be found, the method returns {@code null}.
   *
   * @param variable vertex variable
   * @return vertex or { @code null} if variable is not used
   */
  def getVertexByVariable(variable: String): Option[V] = loader.getVertexByVariable(variable)

  /**
   * Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  def getEdges: Iterable[E] = loader.getEdges

  /**
   * Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  def getEdgesByGraphVariables(variables: String*): Iterable[E] = loader.getEdgesByGraphVariables(variables: _*)

  /**
   * Returns the edge which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable edge variable
   * @return edge or { @code null} if variable is not used
   */
  def getEdgeByVariable(variable: String): Option[E] = loader.getEdgeByVariable(variable)
}

object SparkAsciiGraphLoader {

  /** Initializes the database from the given ASCII GDL string.
   *
   * @param asciiGraphs GDL string (must not be {@code null})
   */
  def fromString[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
  (config: GradoopSparkConfig[G, V, E, LG, GC], asciiGraphs: String): SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    val loader = AsciiGraphLoader.fromString(config.getLogicalGraphFactory, asciiGraphs)
    new SparkAsciiGraphLoader(config, loader)
  }

  /** Initializes the database from the given GDL file.
   *
   * @param fileName GDL file name (must not be {@code null})
   */
  def fromFile[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]]
  (config: GradoopSparkConfig[G, V, E, LG, GC], fileName: String): SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    val loader = AsciiGraphLoader.fromFile(config.getLogicalGraphFactory, fileName)
    new SparkAsciiGraphLoader(config, loader)
  }
}