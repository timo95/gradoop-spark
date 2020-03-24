package org.gradoop.spark.model.impl.operators.tostring.gve

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.operators.tostring.gve.CanonicalAdjacencyMatrixBuilder.LINE_SEPARATOR
import org.gradoop.spark.model.impl.operators.tostring.gve.Functions._
import org.gradoop.spark.model.impl.types.Gve

import scala.collection.TraversableOnce

/** Local implementation of a adjacency matrix builder. Used for graph equality.
 *
 * @param graphHeadToString create string from graph head
 * @param vertexToString create string from vertex
 * @param edgeToString create string from edge
 * @param directed consider edge direction
 * @tparam L layout type
 */
class CanonicalAdjacencyMatrixBuilder[L <: Gve[L]](graphHeadToString: L#G => GraphHeadString,
  vertexToString: L#V => TraversableOnce[VertexString],
  edgeToString: L#E => TraversableOnce[EdgeString],
  directed: Boolean)
  extends UnaryGraphCollectionToValueOperator[L#GC, String] with UnaryLogicalGraphToValueOperator[L#LG, String] {

  override def execute(collection: L#GC): String = {
    getGraphStrings(collection.layout, collection.config)
      .sorted.mkString(LINE_SEPARATOR)
  }

  override def execute(graph: L#LG): String = {
    val graphStrings = getGraphStrings(graph.layout, graph.config)
    if (graphStrings.isEmpty) ""
    else graphStrings.head
  }

  private def getGraphStrings(gveLayout: L#L, config: GradoopSparkConfig[L]): Array[String] = {
    implicit val session: SparkSession = config.sparkSession
    import session.implicits._

    // 1. extract string representations of elements - collect here to local machine
    val graphHeadStrings = gveLayout.graphHeads.map(graphHeadToString).collect
    var vertexStrings = gveLayout.vertices.flatMap(vertexToString).collect
    var edgeStrings = gveLayout.edges.flatMap(edgeToString).collect

    if(directed) {
      // 2. combine strings of parallel edges
      edgeStrings = edgeStrings
        .groupBy(e => e.graphId.toString + e.sourceId.toString + e.targetId.toString).toArray
        .map(t => concatElementStrings(t._2, "&"))

      // 3. extend edge strings by vertex strings (2 joins, max one match each)
      edgeStrings.foreach(e => vertexStrings.foreach(v => {
        if(e.graphId == v.graphId && e.sourceId == v.id) e.sourceString = v.string
        if(e.graphId == v.graphId && e.targetId == v.id) e.targetString = v.string
      }))

      // 4. extend vertex strings by outgoing vertex+edge strings
      val outgoingAdjacencyListStrings = edgeStrings
        .groupBy(e => e.graphId.toString + e.sourceId.toString).toArray
        .map(t => adjacencyList(t._2, _.sourceId, e => s"$LINE_SEPARATOR  -${e.string}->${e.targetString}"))

      // 5. extend vertex strings by incoming vertex+edge strings
      val incomingAdjacencyListStrings = edgeStrings
        .groupBy(e => e.graphId.toString + e.targetId.toString).toArray
        .map(t => adjacencyList(t._2, _.targetId, e => s"$LINE_SEPARATOR  <-${e.string}-${e.sourceString}"))

      // 6. combine vertex strings (2 left joins, max one match each)
      vertexStrings.foreach(v => {
        outgoingAdjacencyListStrings.foreach(ad => if(v.graphId == ad.graphId && v.id == ad.id) v.string += ad.string)
        incomingAdjacencyListStrings.foreach(ad => if(v.graphId == ad.graphId && v.id == ad.id) v.string += ad.string)
      })

    } else {
      // 2. union edges with flipped edges and combine strings of parallel edges
      edgeStrings = edgeStrings
        .union(edgeStrings.map(switchSourceTargetIds))
        .groupBy(e => e.graphId.toString + e.sourceId.toString + e.targetId.toString).toArray
        .map(t => concatElementStrings(t._2, "&"))

      // 3. extend edge strings by vertex strings (join, multiple matches)
      edgeStrings = edgeStrings.flatMap(e => vertexStrings.filter(v => e.graphId == v.graphId && e.targetId == v.id)
        .map(s => EdgeString(e.graphId, e.sourceId, e.targetId, e.sourceString, e.string, s.string)))

      // 4/5. extend vertex strings by vertex+edge strings
      val adjacencyListStrings = edgeStrings
        .groupBy(e => e.graphId.toString + e.sourceId.toString)
        .map(t => adjacencyList(t._2, _.sourceId, e => s"$LINE_SEPARATOR  -${e.string}-${e.targetString}"))

      // 6. combine vertex strings (left join, possibly multiple matches)
      vertexStrings = vertexStrings.flatMap(v => {
        val result = adjacencyListStrings
          .filter(ad => v.graphId == ad.graphId && v.id == ad.id)
          .map(ad => VertexString(v.graphId, v.id, v.string + ad.string))
        if(result.isEmpty) Traversable(v)
        else result
      })
    }

    // 7. create adjacency matrix strings
    val adjacencyMatrixStrings = vertexStrings
      .groupBy(_.graphId).toArray
      .map(t => adjacencyMatrix(t._1, t._2))

    // 8. combine graph strings (left join, max one match)
    graphHeadStrings.foreach(g => adjacencyMatrixStrings
      .foreach(am => if(am.id == g.id) g.string += am.string))
    graphHeadStrings.map(_.string)
  }
}

object CanonicalAdjacencyMatrixBuilder {
  val LINE_SEPARATOR = "\n"
}