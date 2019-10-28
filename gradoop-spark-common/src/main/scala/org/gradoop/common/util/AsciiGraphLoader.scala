package org.gradoop.common.util

import org.gradoop.common.model.api.elements._
import org.s1ck.gdl.GDLHandler

import scala.collection.mutable

class AsciiGraphLoader[G <: GraphHead, V <: Vertex, E <: Edge]
(gdlHandler: GDLHandler, graphHeadFactory: GraphHeadFactory[G], vertexFactory: VertexFactory[V], edgeFactory: EdgeFactory[E]) {

  /** Stores all graphs contained in the GDL script. */
  private val graphHeads: mutable.Map[Id, G] = mutable.HashMap.empty

  /** Mapping between GDL ids and Gradoop IDs. */
  private val graphHeadIds: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores all vertices contained in the GDL script. */
  private val vertices: mutable.Map[Id, V] = mutable.HashMap.empty

  private val vertexIds: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores all edges contained in the GDL script. */
  private val edges: mutable.Map[Id, E] = mutable.HashMap.empty

  private val edgeIds: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores graphs that are assigned to a variable. */
  private val graphHeadCache: mutable.Map[String, G] = mutable.HashMap.empty

  /** Stores vertices that are assigned to a variable. */
  private val vertexCache: mutable.Map[String, V] = mutable.HashMap.empty

  /** Stores edges that are assigned to a variable. */
  private val edgeCache: mutable.Map[String, E] = mutable.HashMap.empty


  /** Appends the given ASCII GDL to the graph handled by that loader.
   *
   * Variables that were previously used, can be reused in the given script and
   * refer to the same entities.
   *
   * @param asciiGraph GDL string
   */
  def appendFromString(asciiGraph: String): Unit = {
    this.gdlHandler.append(asciiGraph)
    init()
  }

  // ---------------------------------------------------------------------------
  //  Graph methods


  /** Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  def getGraphHeads: Iterable[G] = graphHeads.values

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or { @code null} if graph is not cached
   */
  def getGraphHeadByVariable(variable: String): Option[G] = getGraphHeadCache.get(variable)

  /** Returns GraphHeads by their given variables.
   *
   * @param variables variables used in GDL script
   * @return graphHeads that are assigned to the given variables
   */
  def getGraphHeadsByVariables(variables: String*): Set[G] = {
    val result: mutable.Set[G] = mutable.Set[G]()
    
    variables.map(getGraphHeadByVariable).filter(g => g.isDefined).map(g => g.get).foreach(result.add)

    result.toSet
  }


  //  Vertex methods


  /** Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  def getVertices: Iterable[V] = vertices.values

  /** Returns vertex by its given variable.
   *
   * @param variable variable used in GDL script
   * @return vertex or { @code null} if not present
   */
  def getVertexByVariable(variable: String): Option[V] = vertexCache.get(variable)

  /** Returns vertices by their given variables.
   *
   * @param variables variables used in GDL script
   * @return vertices
   */
  def getVerticesByVariables(variables: String*): Set[V] = {
    val result: mutable.Set[V] = mutable.Set[V]()

    variables.map(getVertexByVariable).filter(v => v.isDefined).map(v => v.get).foreach(result.add)

    result.toSet
  }

  /** Returns all vertices that belong to the given graphs.
   *
   * @param graphIds graph identifiers
   * @return vertices that are contained in the graphs
   */
  def getVerticesByGraphIds(graphIds: IdSet): Set[V] = {
    val result: mutable.Set[V] = mutable.Set[V]()

    for (vertex <- vertices.values) {
      if (graphIds.exists(vertex.getGraphIds)) result.add(vertex)
    }

    result.toSet
  }

  /** Returns all vertices that belong to the given graph variables.
   *
   * @param graphVariables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  def getVerticesByGraphVariables(graphVariables: String*): Set[V] = {
    val graphIds: mutable.Set[Id] = mutable.Set[Id]()
    for (graphHead <- getGraphHeadsByVariables(graphVariables:_*)) {
      graphIds.add(graphHead.getId)
    }
    getVerticesByGraphIds(graphIds.toSet)
  }


  //  Edge methods


  /** Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  def getEdges: Iterable[E] = edges.values

  /**
   * Returns edge by its given variable.
   *
   * @param variable variable used in GDL script
   * @return edge or { @code null} if not present
   */
  def getEdgeByVariable(variable: String): Option[E] = edgeCache.get(variable)

  /** Returns edges by their given variables.
   *
   * @param variables variables used in GDL script
   * @return edges
   */
  def getEdgesByVariables(variables: String*): Iterable[E] = {
    val result: mutable.Set[E] = mutable.Set[E]()//Sets.newHashSetWithExpectedSize(variables.length)
    for (variable <- variables) {
      val edge: Option[E] = edgeCache.get(variable)
      edge.map(result.add)
    }
    result
  }

  /** Returns all edges that belong to the given graphs.
   *
   * @param graphIds Graph identifiers
   * @return edges
   */
  def getEdgesByGraphIds(graphIds: IdSet): Iterable[E] = {
    val result: mutable.Set[E] = mutable.Set[E]()//Sets.newHashSetWithExpectedSize(graphIds.size)
    for (edge <- edges.values) {
      if (graphIds.exists(edge.getGraphIds.contains)) result.add(edge)
    }
    result
  }

  /** Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  def getEdgesByGraphVariables(variables: String*): Iterable[E] = {
    val graphIds: mutable.Set[Id] = mutable.Set[Id]()
    for (graphHead <- getGraphHeadsByVariables(variables:_*)) {
      graphIds.add(graphHead.getId)
    }
    getEdgesByGraphIds(graphIds.toSet)
  }


  //  Caches


  /** Returns all graph heads that are bound to a variable in the GDL script.
   *
   * @return variable to graphHead mapping
   */
  def getGraphHeadCache: Map[String, G] = graphHeadCache.toMap

  /** Returns all vertices that are bound to a variable in the GDL script.
   *
   * @return variable to vertex mapping
   */
  def getVertexCache: Map[String, V] = vertexCache.toMap

  /** Returns all edges that are bound to a variable in the GDL script.
   *
   * @return variable to edge mapping
   */
  def getEdgeCache: Map[String, E] = edgeCache.toMap


  /** Initializes the AsciiGraphLoader */
  private def init(): Unit = {
    initGraphHeads()
    initVertices()
    initEdges()
  }

  /** Initializes GraphHeads and their cache. */
  private def initGraphHeads(): Unit = {
    import scala.collection.JavaConversions._
    for (g <- gdlHandler.getGraphs) {
      if (!graphHeadIds.containsKey(g.getId)) initGraphHead(g)
    }
    import scala.collection.JavaConversions._
    for (e <- gdlHandler.getGraphCache.entrySet) {
      updateGraphCache(e.getKey, e.getValue)
    }
  }

  /** Initializes vertices and their cache. */
  private def initVertices(): Unit = {
    import scala.collection.JavaConversions._
    for (v <- gdlHandler.getVertices) {
      initVertex(v)
    }
    import scala.collection.JavaConversions._
    for (e <- gdlHandler.getVertexCache.entrySet) {
      updateVertexCache(e.getKey, e.getValue)
    }
  }

  /** Initializes edges and their cache. */
  private def initEdges(): Unit = {
    import scala.collection.JavaConversions._
    for (e <- gdlHandler.getEdges) {
      initEdge(e)
    }
    import scala.collection.JavaConversions._
    for (e <- gdlHandler.getEdgeCache.entrySet) {
      updateEdgeCache(e.getKey, e.getValue)
    }
  }

  /** Creates a new Graph from the GDL Loader.
   *
   * @param g graph from GDL Loader
   * @return graph head
   */
  private def initGraphHead(g: org.s1ck.gdl.model.Graph): G = {
    val properties: Properties = "" //Properties.createFromMap(g.getProperties)
    val graphHead: G = graphHeadFactory.create(g.getLabel.split(GradoopConstants.LABEL_DELIMITER), properties)
    graphHeadIds.put(g.getId, graphHead.getId)
    graphHeads.put(graphHead.getId, graphHead)
    graphHead
  }

  /** Creates a new Vertex from the GDL Loader or updates an existing one.
   *
   * @param v vertex from GDL Loader
   * @return vertex
   */
  private def initVertex(v: org.s1ck.gdl.model.Vertex): V = {
    if (!vertexIds.contains(v.getId)) {
      val properties: Properties = "" //Properties.createFromMap(v.getProperties)
      val vertex: V = vertexFactory.create(v.getLabel.split(GradoopConstants.LABEL_DELIMITER), properties, createGradoopIdSet(v))
      vertexIds.put(v.getId, vertex.getId)
      vertices.put(vertex.getId, vertex)
      vertex
    }
    else {
      val vertex: V = vertices(vertexIds(v.getId))
      vertex.setGraphIds(createGradoopIdSet(v))
      vertex
    }
  }

  /** Creates a new Edge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return edge
   */
  private def initEdge(e: org.s1ck.gdl.model.Edge): E = {
    if (!edgeIds.contains(e.getId)) {
      val properties: Properties = "" //Properties.createFromMap(e.getProperties)
      val edge: E = edgeFactory.create(e.getLabel.split(GradoopConstants.LABEL_DELIMITER), vertexIds(e.getSourceVertexId),
        vertexIds(e.getTargetVertexId), properties, createGradoopIdSet(e))
      edgeIds.put(e.getId, edge.getId)
      edges.put(edge.getId, edge)
      edge
    }
    else {
      val edge: E = edges(edgeIds(e.getId))
      edge.setGraphIds(createGradoopIdSet(e))
      edge
    }
  }

  /** Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g        graph from GDL loader
   */
  private def updateGraphCache(variable: String, g: org.s1ck.gdl.model.Graph): Unit = {
    graphHeadCache + (variable -> graphHeads(graphHeadIds(g.getId)))
  }

  /** Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v        vertex from GDL loader
   */
  private def updateVertexCache(variable: String, v: org.s1ck.gdl.model.Vertex): Unit = {
    vertexCache + (variable -> vertices(vertexIds(v.getId)))
  }

  /** Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e        edge from GDL loader
   */
  private def updateEdgeCache(variable: String, e: org.s1ck.gdl.model.Edge): Unit = {
    edgeCache + (variable -> edges(edgeIds(e.getId)))
  }

  /** Creates a {@code GradoopIDSet} from the long identifiers stored at the given graph element.
   *
   * @param e graph element
   * @return GradoopIDSet for the given element
   */
  private def createGradoopIdSet(e: org.s1ck.gdl.model.GraphElement): IdSet = {
    import collection.JavaConverters._
    e.getGraphs.asScala.map(id => graphHeadIds(id)).toSet
  }

}


object AsciiGraphLoader {

  def apply[G <: GraphHead, V <: Vertex, E <: Edge](gdlHandler: GDLHandler, elementFactoryProvider: ElementFactoryProvider[G, V, E]): AsciiGraphLoader[G, V, E] = {
    new AsciiGraphLoader[G, V, E](gdlHandler, elementFactoryProvider.getGraphHeadFactory, elementFactoryProvider.getVertexFactory,
      elementFactoryProvider.getEdgeFactory)
  }
}