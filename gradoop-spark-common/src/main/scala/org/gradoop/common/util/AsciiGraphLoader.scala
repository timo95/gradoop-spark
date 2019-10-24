package org.gradoop.common.util

import java.util

import org.gradoop.common.model.api.elements._
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.{Graph, GraphElement}

import scala.collection.mutable

class AsciiGraphLoader[G <: GraphHead, V <: Vertex, E <: Edge]
(gdlHandler: GDLHandler, graphHeadFactory: GraphHeadFactory[G], vertexFactory: VertexFactory[V], edgeFactory: EdgeFactory[E]) {

  /** Stores all graphs contained in the GDL script. */
  private var graphHeads: mutable.Map[Id, G] = mutable.HashMap.empty

  /** Mapping between GDL ids and Gradoop IDs. */
  private var graphHeadIds: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores all vertices contained in the GDL script. */
  private var vertices: mutable.Map[Id, V] = mutable.HashMap.empty

  private var vertexIds: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores all edges contained in the GDL script. */
  private var edges: mutable.Map[Id, E] = mutable.HashMap.empty

  private var edgeIds: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores graphs that are assigned to a variable. */
  private var graphHeadCache: mutable.Map[String, G] = mutable.HashMap.empty

  /** Stores vertices that are assigned to a variable. */
  private var vertexCache: mutable.Map[String, V] = mutable.HashMap.empty

  /** Stores edges that are assigned to a variable. */
  private var edgeCache: mutable.Map[String, E] = mutable.HashMap.empty


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
  def getGraphHeads: util.Collection[G] = new ImmutableSet.Builder[G]().addAll(graphHeads.values).build

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or { @code null} if graph is not cached
   */
  def getGraphHeadByVariable(variable: String): G = getGraphHeadCache.get(variable)

  /** Returns GraphHeads by their given variables.
   *
   * @param variables variables used in GDL script
   * @return graphHeads that are assigned to the given variables
   */
  def getGraphHeadsByVariables(variables: String*): util.Collection[G] = {
    val result: util.Collection[G] = Sets.newHashSetWithExpectedSize(variables.length)
    for (variable <- variables) {
      val graphHead: G = getGraphHeadByVariable(variable)
      if (graphHead != null) result.add(graphHead)
    }
    result
  }


  //  Vertex methods


  /** Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  def getVertices: util.Collection[V] = new ImmutableSet.Builder[V]().addAll(vertices.values).build

  /** Returns vertex by its given variable.
   *
   * @param variable variable used in GDL script
   * @return vertex or { @code null} if not present
   */
  def getVertexByVariable(variable: String): V = vertexCache.get(variable)

  /** Returns vertices by their given variables.
   *
   * @param variables variables used in GDL script
   * @return vertices
   */
  def getVerticesByVariables(variables: String*): util.Collection[V] = {
    val result: util.Collection[V] = Sets.newHashSetWithExpectedSize(variables.length)
    for (variable <- variables) {
      val vertex: V = getVertexByVariable(variable)
      if (vertex != null) result.add(vertex)
    }
    result
  }

  /** Returns all vertices that belong to the given graphs.
   *
   * @param graphIds graph identifiers
   * @return vertices that are contained in the graphs
   */
  def getVerticesByGraphIds(graphIds: GradoopIdSet): util.Collection[V] = {
    val result: util.Collection[V] = Sets.newHashSetWithExpectedSize(graphIds.size)
    for (vertex <- vertices.values) {
      if (vertex.getGraphIds.containsAny(graphIds)) result.add(vertex)
    }
    result
  }

  /** Returns all vertices that belong to the given graph variables.
   *
   * @param graphVariables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  def getVerticesByGraphVariables(graphVariables: String*): util.Collection[V] = {
    val graphIds: GradoopIdSet = new GradoopIdSet
    import scala.collection.JavaConversions._
    for (graphHead <- getGraphHeadsByVariables(graphVariables)) {
      graphIds.add(graphHead.getId)
    }
    getVerticesByGraphIds(graphIds)
  }


  //  Edge methods


  /** Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  def getEdges: util.Collection[E] = new ImmutableSet.Builder[E]().addAll(edges.values).build

  /**
   * Returns edge by its given variable.
   *
   * @param variable variable used in GDL script
   * @return edge or { @code null} if not present
   */
  def getEdgeByVariable(variable: String): E = edgeCache.get(variable)

  /** Returns edges by their given variables.
   *
   * @param variables variables used in GDL script
   * @return edges
   */
  def getEdgesByVariables(variables: String*): util.Collection[E] = {
    val result: util.Collection[E] = Sets.newHashSetWithExpectedSize(variables.length)
    for (variable <- variables) {
      val edge: E = edgeCache.get(variable)
      if (edge != null) result.add(edge)
    }
    result
  }

  /** Returns all edges that belong to the given graphs.
   *
   * @param graphIds Graph identifiers
   * @return edges
   */
  def getEdgesByGraphIds(graphIds: GradoopIdSet): util.Collection[E] = {
    val result: util.Collection[E] = Sets.newHashSetWithExpectedSize(graphIds.size)
    for (edge <- edges.values) {
      if (edge.getGraphIds.containsAny(graphIds)) result.add(edge)
    }
    result
  }

  /** Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  def getEdgesByGraphVariables(variables: String*): util.Collection[E] = {
    val graphIds: GradoopIdSet = new GradoopIdSet
    import scala.collection.JavaConversions._
    for (graphHead <- getGraphHeadsByVariables(variables)) {
      graphIds.add(graphHead.getId)
    }
    getEdgesByGraphIds(graphIds)
  }


  //  Caches


  /** Returns all graph heads that are bound to a variable in the GDL script.
   *
   * @return variable to graphHead mapping
   */
  def getGraphHeadCache: util.Map[String, G] = graphHeadCache.toMap

  /** Returns all vertices that are bound to a variable in the GDL script.
   *
   * @return variable to vertex mapping
   */
  def getVertexCache: util.Map[String, V] = vertexCache.toMap

  /** Returns all edges that are bound to a variable in the GDL script.
   *
   * @return variable to edge mapping
   */
  def getEdgeCache: util.Map[String, E] = edgeCache.toMap


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
  private def initGraphHead(g: org.s1ck.gdl.model.Graph) = {
    val graphHead = graphHeadFactory.create(g.getLabel, Properties.createFromMap(g.getProperties))
    graphHeadIds.put(g.getId, graphHead.getId)
    graphHeads.put(graphHead.getId, graphHead)
    graphHead
  }

  /** Creates a new Vertex from the GDL Loader or updates an existing one.
   *
   * @param v vertex from GDL Loader
   * @return vertex
   */
  private def initVertex(v: org.s1ck.gdl.model.Vertex) = {
    var vertex = null
    if (!vertexIds.containsKey(v.getId)) {
      vertex = vertexFactory.create(v.getLabel, Properties.createFromMap(v.getProperties), createGradoopIdSet(v))
      vertexIds.put(v.getId, vertex.getId)
      vertices.put(vertex.getId, vertex)
    }
    else {
      vertex = vertices.get(vertexIds.get(v.getId))
      vertex.setGraphIds(createGradoopIdSet(v))
    }
    vertex
  }

  /** Creates a new Edge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return edge
   */
  private def initEdge(e: org.s1ck.gdl.model.Edge) = {
    var edge = null
    if (!edgeIds.containsKey(e.getId)) {
      edge = edgeFactory.create(e.getLabel, vertexIds.get(e.getSourceVertexId), vertexIds.get(e.getTargetVertexId), Properties.createFromMap(e.getProperties), createGradoopIdSet(e))
      edgeIds.put(e.getId, edge.getId)
      edges.put(edge.getId, edge)
    }
    else {
      edge = edges.get(edgeIds.get(e.getId))
      edge.setGraphIds(createGradoopIdSet(e))
    }
    edge
  }

  /** Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g        graph from GDL loader
   */
  private def updateGraphCache(variable: String, g: org.s1ck.gdl.model.Graph): Unit = {
    graphHeadCache + (variable -> graphHeads.get(graphHeadIds.get(g.getId)))
  }

  /** Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v        vertex from GDL loader
   */
  private def updateVertexCache(variable: String, v: org.s1ck.gdl.model.Vertex): Unit = {
    vertexCache + (variable -> vertices.get(vertexIds.get(v.getId)))
  }

  /** Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e        edge from GDL loader
   */
  private def updateEdgeCache(variable: String, e: org.s1ck.gdl.model.Edge): Unit = {
    edgeCache + (variable -> edges.get(edgeIds.get(e.getId)))
  }

  /** Creates a {@code GradoopIDSet} from the long identifiers stored at the given graph element.
   *
   * @param e graph element
   * @return GradoopIDSet for the given element
   */
  private def createGradoopIdSet(e: GraphElement): IdSet = {
    val result = new GradoopIdSet
    import scala.collection.JavaConversions._
    for (graphId <- e.getGraphs) {
      result.add(graphHeadIds.get(graphId))
    }
    result
  }

}


object AsciiGraphLoader {

  def apply[G <: GraphHead, V <: Vertex, E <: Edge](gdlHandler: GDLHandler, elementFactoryProvider: ElementFactoryProvider[G, V, E]): AsciiGraphLoader[G, V, E] = {
    new AsciiGraphLoader[G, V, E](gdlHandler, elementFactoryProvider.getGraphHeadFactory, elementFactoryProvider.getVertexFactory,
      elementFactoryProvider.getEdgeFactory)
  }
}