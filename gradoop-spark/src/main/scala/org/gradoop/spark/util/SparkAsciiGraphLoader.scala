package org.gradoop.spark.util

import java.io.{IOException, InputStream}
import java.util

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.types.GraphModel


class SparkAsciiGraphLoader(config: GradoopSparkConfig[G, V, E, LG, GC]) extends GraphModel {

  /**
   * AsciiGraphLoader to create graph, vertex and edge collections.
   */
  private var loader = new AsciiGraphLoader[G, V, E]()

  /**
   * Initializes the database from the given ASCII GDL string.
   *
   * @param asciiGraphs GDL string (must not be { @code null})
   */
  def initDatabaseFromString(asciiGraphs: String): Unit = {
    if (asciiGraphs == null) throw new IllegalArgumentException("AsciiGraph must not be null")
    loader = AsciiGraphLoader.fromString(asciiGraphs, config.getLogicalGraphFactory)
  }

  /**
   * Initializes the database from the given ASCII GDL stream.
   *
   * @param stream GDL stream
   * @throws IOException on failure
   */
  @throws[IOException]
  def initDatabaseFromStream(stream: InputStream): Unit = {
    if (stream == null) throw new IllegalArgumentException("AsciiGraph must not be null")
    loader = AsciiGraphLoader.fromStream(stream, config.getLogicalGraphFactory)
  }

  /**
   * Appends the given ASCII GDL String to the database.
   *
   * Variables previously used can be reused as their refer to the same objects.
   *
   * @param asciiGraph GDL string (must not be { @code null})
   */
  def appendToDatabaseFromString(asciiGraph: String): Unit = {
    if (asciiGraph == null) throw new IllegalArgumentException("AsciiGraph must not be null")
    if (loader != null) loader.appendFromString(asciiGraph)
    else initDatabaseFromString(asciiGraph)
  }

  /**
   * Initializes the database from the given GDL file.
   *
   * @param fileName GDL file name (must not be { @code null})
   * @throws IOException on failure
   */
  @throws[IOException]
  def initDatabaseFromFile(fileName: String): Unit = {
    if (fileName == null) throw new IllegalArgumentException("FileName must not be null.")
    loader = AsciiGraphLoader.fromFile(fileName, config.getLogicalGraphFactory)
  }

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * the database.
   * This is equivalent to {@link #getLogicalGraph(boolean) getLogicalGraph(true)}.
   *
   * @return logical graph of vertex and edge space
   */
  def getLogicalGraph: LogicalGraph = getLogicalGraph(true)

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * the database.
   *
   * @param withGraphContainment true, if vertices and edges shall be updated to
   *                             be contained in the logical graph representing
   *                             the database
   * @return logical graph of vertex and edge space
   */
  def getLogicalGraph(withGraphContainment: Boolean): LogicalGraph = {
    val factory = config.getLogicalGraphFactory
    if (withGraphContainment) factory.fromCollections(getVertices, getEdges).transformGraphHead(new RenameLabel[GraphHead](GradoopConstants.DEFAULT_GRAPH_LABEL, GradoopConstants.DB_GRAPH_LABEL))
    else {
      val graphHead = factory.getGraphHeadFactory.createGraphHead(GradoopConstants.DB_GRAPH_LABEL)
      factory.fromCollections(graphHead, getVertices, getEdges)
    }
  }

  /**
   * Builds a {@link LogicalGraph} from the graph referenced by the given
   * graph variable.
   *
   * @param variable graph variable used in GDL script
   * @return LogicalGraph
   */
  def getLogicalGraphByVariable(variable: String): LogicalGraph = {
    val graphHead = getGraphHeadByVariable(variable)
    val vertices = getVerticesByGraphVariables(variable)
    val edges = getEdgesByGraphVariables(variable)
    config.getLogicalGraphFactory.fromCollections(graphHead, vertices, edges)
  }

  /**
   * Returns a collection of all logical graph contained in the database.
   *
   * @return collection of all logical graphs
   */
  def getGraphCollection: GraphCollection = {
    val session = config.getSparkSession
    val newVertices = env.fromCollection(getVertices).filter((vertex: V) => vertex.getGraphCount > 0)
    val newEdges = env.fromCollection(getEdges).filter((edge: E) => edge.getGraphCount > 0)
    config.getGraphCollectionFactory.fromDatasets(env.fromCollection(getGraphHeads), newVertices, newEdges)
  }

  /**
   * Builds a {@link GraphCollection} from the graph referenced by the given
   * graph variables.
   *
   * @param variables graph variables used in GDL script
   * @return GraphCollection
   */
  def getGraphCollectionByVariables(variables: String*): GraphCollection = {
    val graphHeads = getGraphHeadsByVariables(variables: _*)
    val vertices = getVerticesByGraphVariables(variables: _*)
    val edges = getEdgesByGraphVariables(variables: _*)
    config.getGraphCollectionFactory.fromCollections(graphHeads, vertices, edges)
  }

  /**
   * Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  def getGraphHeads: util.Collection[G] = loader.getGraphHeads

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or { @code null} if graph is not cached
   */
  def getGraphHeadByVariable(variable: String): G = loader.getGraphHeadByVariable(variable)

  /**
   * Returns the graph heads assigned to the specified variables.
   *
   * @param variables variables used in the GDL script
   * @return graphHeads assigned to the variables
   */
  def getGraphHeadsByVariables(variables: String*): Iterable[G] = loader.getGraphHeadsByVariables(variables: _*).asScala

  /**
   * Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  def getVertices: Iterable[V] = loader.getVertices.asScala

  /**
   * Returns all vertices that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  def getVerticesByGraphVariables(variables: String*): Iterable[V] = loader.getVerticesByGraphVariables(variables: _*).asScala

  /**
   * Returns the vertex which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable vertex variable
   * @return vertex or { @code null} if variable is not used
   */
  def getVertexByVariable(variable: String): V = loader.getVertexByVariable(variable)

  /**
   * Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  def getEdges: Iterable[E] = loader.getEdges.asScala

  /**
   * Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  def getEdgesByGraphVariables(variables: String*): Iterable[E] = loader.getEdgesByGraphVariables(variables: _*).asScala

  /**
   * Returns the edge which is identified by the given variable. If the
   * variable cannot be found, the method returns {@code null}.
   *
   * @param variable edge variable
   * @return edge or { @code null} if variable is not used
   */
  def getEdgeByVariable(variable: String): E = loader.getEdgeByVariable(variable)
}
