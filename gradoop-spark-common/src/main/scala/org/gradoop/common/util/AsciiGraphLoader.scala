package org.gradoop.common.util

import java.io.InputStream

import org.gradoop.common.model.api.gve._
import org.gradoop.common.properties.PropertyValue
import org.s1ck.gdl.GDLHandler

import scala.collection.mutable

class AsciiGraphLoader[G <: GveGraphHead, V <: GveVertex, E <: GveEdge]
(gdlHandler: GDLHandler, graphHeadFactory: GveGraphHeadFactory[G],
 vertexFactory: GveVertexFactory[V], edgeFactory: GveEdgeFactory[E]) {

  /** Stores all graphs contained in the GDL script. */
  private val idToGraphHead: mutable.Map[Id, G] = mutable.HashMap.empty

  /** Stores all vertices contained in the GDL script. */
  private val idToVertex: mutable.Map[Id, V] = mutable.HashMap.empty

  /** Stores all edges contained in the GDL script. */
  private val idToEdge: mutable.Map[Id, E] = mutable.HashMap.empty

  /** Mapping between GDL ids and Gradoop IDs. */
  private val graphHeadIdMapping: mutable.Map[Long, Id] = mutable.HashMap.empty
  private val vertexIdMapping: mutable.Map[Long, Id] = mutable.HashMap.empty
  private val edgeIdMapping: mutable.Map[Long, Id] = mutable.HashMap.empty

  /** Stores graphs that are assigned to a variable. */
  private val graphHeadCache: mutable.Map[String, G] = mutable.HashMap.empty

  /** Stores vertices that are assigned to a variable. */
  private val vertexCache: mutable.Map[String, V] = mutable.HashMap.empty

  /** Stores edges that are assigned to a variable. */
  private val edgeCache: mutable.Map[String, E] = mutable.HashMap.empty

  init()

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

  //  Graph methods

  /** Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  def graphHeads: Iterable[G] = idToGraphHead.values

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or { @code null} if graph is not cached
   */
  def graphHeadsByVariable(variable: String): Option[G] = getGraphHeadCache.get(variable)

  /** Returns GraphHeads by their given variables.
   *
   * @param variables variables used in GDL script
   * @return graphHeads that are assigned to the given variables
   */
  def graphHeadsByVariables(variables: String*): Set[G] = {
    val result: mutable.Set[G] = mutable.Set[G]()
    variables.map(graphHeadsByVariable).filter(g => g.isDefined).map(g => g.get).foreach(result.add)

    result.toSet
  }

  //  Vertex methods

  /** Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  def vertices: Iterable[V] = idToVertex.values

  /** Returns vertex by its given variable.
   *
   * @param variable variable used in GDL script
   * @return vertex or { @code null} if not present
   */
  def verticesByVariable(variable: String): Option[V] = vertexCache.get(variable)

  /** Returns vertices by their given variables.
   *
   * @param variables variables used in GDL script
   * @return vertices
   */
  def verticesByVariables(variables: String*): Set[V] = {
    val result: mutable.Set[V] = mutable.Set[V]()
    variables.map(verticesByVariable).filter(v => v.isDefined).map(v => v.get).foreach(result.add)

    result.toSet
  }

  /** Returns all vertices that belong to the given graphs.
   *
   * @param graphIds graph identifiers
   * @return vertices that are contained in the graphs
   */
  def verticesByGraphIds(graphIds: IdSet): Set[V] = {
    val result: mutable.Set[V] = mutable.Set[V]()
    for (vertex <- idToVertex.values) {
      if (graphIds.exists(vertex.graphIds)) result.add(vertex)
    }

    result.toSet
  }

  /** Returns all vertices that belong to the given graph variables.
   *
   * @param graphVariables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  def verticesByGraphVariables(graphVariables: String*): Set[V] = {
    val graphIds: mutable.Set[Id] = mutable.Set[Id]()
    for (graphHead <- graphHeadsByVariables(graphVariables:_*)) {
      graphIds.add(graphHead.id)
    }
    verticesByGraphIds(graphIds.toSet)
  }


  //  Edge methods


  /** Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  def edges: Iterable[E] = idToEdge.values

  /** Returns edge by its given variable.
   *
   * @param variable variable used in GDL script
   * @return edge or { @code null} if not present
   */
  def edgeByVariable(variable: String): Option[E] = edgeCache.get(variable)

  /** Returns edges by their given variables.
   *
   * @param variables variables used in GDL script
   * @return edges
   */
  def edgesByVariables(variables: String*): Iterable[E] = {
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
  def edgesByGraphIds(graphIds: IdSet): Iterable[E] = {
    val result: mutable.Set[E] = mutable.Set[E]()//Sets.newHashSetWithExpectedSize(graphIds.size)
    for (edge <- idToEdge.values) {
      if (graphIds.exists(edge.graphIds.contains)) result.add(edge)
    }
    result
  }

  /** Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  def edgesByGraphVariables(variables: String*): Iterable[E] = {
    val graphIds: mutable.Set[Id] = mutable.Set[Id]()
    for (graphHead <- graphHeadsByVariables(variables:_*)) {
      graphIds.add(graphHead.id)
    }
    edgesByGraphIds(graphIds.toSet)
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
    import scala.collection.JavaConverters._
    for (graph <- gdlHandler.getGraphs.asScala) {
      if (!graphHeadIdMapping.contains(graph.getId)) initGraphHead(graph)
    }
    for ((key, value) <- gdlHandler.getGraphCache().asScala) {
      updateGraphCache(key, value)
    }
  }

  /** Initializes vertices and their cache. */
  private def initVertices(): Unit = {
    import scala.collection.JavaConverters._
    gdlHandler.getVertices.asScala.foreach(initVertex)
    for ((key, value) <- gdlHandler.getVertexCache.asScala) {
      updateVertexCache(key, value)
    }
  }

  /** Initializes edges and their cache. */
  private def initEdges(): Unit = {
    import scala.collection.JavaConverters._
    gdlHandler.getEdges.asScala.foreach(initEdge)
    for ((key, value) <- gdlHandler.getEdgeCache.asScala) {
      updateEdgeCache(key, value)
    }
  }

  /** Creates a new Graph from the GDL Loader.
   *
   * @param g graph from GDL Loader
   * @return graph head
   */
  private def initGraphHead(g: org.s1ck.gdl.model.Graph): G = {
    import collection.JavaConverters._
    val properties: Properties = g.getProperties.asScala.mapValues(PropertyValue.apply).toMap
    val graphHead: G = graphHeadFactory.create(g.getLabel/*.split(GradoopConstants.LABEL_DELIMITER)*/, properties)
    graphHeadIdMapping.put(g.getId, graphHead.id)
    idToGraphHead.put(graphHead.id, graphHead)
    graphHead
  }

  /** Creates a new Vertex from the GDL Loader or updates an existing one.
   *
   * @param v vertex from GDL Loader
   * @return vertex
   */
  private def initVertex(v: org.s1ck.gdl.model.Vertex): V = {
    if (!vertexIdMapping.contains(v.getId)) {
      import collection.JavaConverters._
      val properties: Properties = v.getProperties.asScala.mapValues(PropertyValue.apply).toMap
      val vertex: V = vertexFactory.create(v.getLabel, properties, createGradoopIdSet(v))
      vertexIdMapping.put(v.getId, vertex.id)
      idToVertex.put(vertex.id, vertex)
      vertex
    }
    else {
      val vertex: V = idToVertex(vertexIdMapping(v.getId))
      vertex.graphIds = createGradoopIdSet(v)
      vertex
    }
  }

  /** Creates a new Edge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return edge
   */
  private def initEdge(e: org.s1ck.gdl.model.Edge): E = {
    if (!edgeIdMapping.contains(e.getId)) {
      import collection.JavaConverters._
      val properties: Properties = e.getProperties.asScala.mapValues(PropertyValue.apply).toMap
      val edge: E = edgeFactory.create(e.getLabel, vertexIdMapping(e.getSourceVertexId),
        vertexIdMapping(e.getTargetVertexId), properties, createGradoopIdSet(e))
      edgeIdMapping.put(e.getId, edge.id)
      idToEdge.put(edge.id, edge)
      edge
    }
    else {
      val edge: E = idToEdge(edgeIdMapping(e.getId))
      edge.graphIds = createGradoopIdSet(e)
      edge
    }
  }

  /** Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g        graph from GDL loader
   */
  private def updateGraphCache(variable: String, g: org.s1ck.gdl.model.Graph): Unit = {
    graphHeadCache + (variable -> idToGraphHead(graphHeadIdMapping(g.getId)))
  }

  /** Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v        vertex from GDL loader
   */
  private def updateVertexCache(variable: String, v: org.s1ck.gdl.model.Vertex): Unit = {
    vertexCache + (variable -> idToVertex(vertexIdMapping(v.getId)))
  }

  /** Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e        edge from GDL loader
   */
  private def updateEdgeCache(variable: String, e: org.s1ck.gdl.model.Edge): Unit = {
    edgeCache + (variable -> idToEdge(edgeIdMapping(e.getId)))
  }

  /** Creates a {@code GradoopIDSet} from the long identifiers stored at the given graph element.
   *
   * @param e graph element
   * @return GradoopIDSet for the given element
   */
  private def createGradoopIdSet(e: org.s1ck.gdl.model.GraphElement): IdSet = {
    import collection.JavaConverters._
    e.getGraphs.asScala.map(id => graphHeadIdMapping(id)).toSet
  }

}

object AsciiGraphLoader {

  def fromString[G <: GveGraphHead, V <: GveVertex, E <: GveEdge]
  (elementFactoryProvider: GveElementFactoryProvider[G, V, E], asciiGraph: String): AsciiGraphLoader[G, V, E] = {
    new AsciiGraphLoader[G, V, E](new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromString(asciiGraph),
      elementFactoryProvider.graphHeadFactory,
      elementFactoryProvider.vertexFactory,
      elementFactoryProvider.edgeFactory)
  }

  def fromFile[G <: GveGraphHead, V <: GveVertex, E <: GveEdge]
  (elementFactoryProvider: GveElementFactoryProvider[G, V, E], fileName: String): AsciiGraphLoader[G, V, E] = {
    new AsciiGraphLoader[G, V, E](new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromFile(fileName),
      elementFactoryProvider.graphHeadFactory,
      elementFactoryProvider.vertexFactory,
      elementFactoryProvider.edgeFactory)
  }

  def fromStream[G <: GveGraphHead, V <: GveVertex, E <: GveEdge]
  (elementFactoryProvider: GveElementFactoryProvider[G, V, E], inputStream: InputStream): AsciiGraphLoader[G, V, E] = {
    new AsciiGraphLoader[G, V, E](new GDLHandler.Builder()
      .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
      .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
      .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL)
      .buildFromStream(inputStream),
      elementFactoryProvider.graphHeadFactory,
      elementFactoryProvider.vertexFactory,
      elementFactoryProvider.edgeFactory)
  }
}
