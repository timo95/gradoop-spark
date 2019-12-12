package org.gradoop.spark.model.impl.operators.tostring.gve

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.Gve

import scala.collection.TraversableOnce

class CanonicalAdjacencyMatrixBuilder[L <: Gve[L]](graphHeadToString: L#G => GraphHeadString,
                                                   vertexToString: L#V => TraversableOnce[VertexString],
                                                   edgeToString: L#E => TraversableOnce[EdgeString],
                                                   directed: Boolean)
  extends UnaryGraphCollectionToValueOperator[L#GC, String] with UnaryLogicalGraphToValueOperator[L#LG, String] {
  import CanonicalAdjacencyMatrixBuilder._

  override def execute(collection: L#GC): String = {
    import collection.config.sparkSession.implicits._

    val graphStrings = getGraphStrings(collection.layout, collection.config)
    if(graphStrings.isEmpty) "" // Collections can be empty
    else {
      graphStrings
        .groupByKey(_ => "")
        .flatMapGroups((_, g) => concatElementStrings(g, System.lineSeparator))
        .first
        .string
    }
  }

  override def execute(graph: L#LG): String = {
    getGraphStrings(graph.layout, graph.config).first.string
  }

  private def getGraphStrings(gveLayout: L#L, config: GradoopSparkConfig[L]): Dataset[GraphHeadString] = {
    implicit val session: SparkSession = config.sparkSession
    import session.implicits._

    // 1. extract string representations of elements
    val graphHeadStrings = gveLayout.graphHeads.map(graphHeadToString)
    var vertexStrings = gveLayout.vertices.flatMap(vertexToString)
    var edgeStrings = gveLayout.edges.flatMap(edgeToString)

    if(directed) {
      // 2. combine strings of parallel edges
      edgeStrings = edgeStrings
        .groupByKey(e => e.graphId.toString + e.sourceId.toString + e.targetId.toString)
        .flatMapGroups((_, e) => concatElementStrings(e, "&"))

      // 3. extend edge strings by vertex strings
      edgeStrings = edgeStrings
        .joinWith(vertexStrings,
          edgeStrings("graphId") === vertexStrings("graphId") and
          edgeStrings("sourceId") === vertexStrings("id"))
        .map(updateSourceString)
      edgeStrings = edgeStrings
        .joinWith(vertexStrings,
          edgeStrings("graphId") === vertexStrings("graphId") and
          edgeStrings("targetId") === vertexStrings("id"))
        .map(updateTargetString)

      // 4. extend vertex strings by outgoing vertex+edge strings
      val outgoingAdjacencyListStrings = edgeStrings
        .groupByKey(e => e.graphId.toString + e.sourceId.toString)
        .flatMapGroups((_, it) => adjacencyList(it, _.sourceId, e => s"\n  -${e.string}->${e.targetString}"))

      // 5. extend vertex strings by outgoing vertex+edge strings
      val incomingAdjacencyListStrings = edgeStrings
        .groupByKey(e => e.graphId.toString + e.targetId.toString)
        .flatMapGroups((_, it) => adjacencyList(it, _.targetId, e => s"\n  <-${e.string}-${e.sourceString}"))

      // 6. combine vertex strings
      vertexStrings = vertexStrings
        .joinWith(outgoingAdjacencyListStrings,
          vertexStrings("graphId") === outgoingAdjacencyListStrings("graphId") and
          vertexStrings("id") === outgoingAdjacencyListStrings("id"),
          "left")
        .map(combineElementStrings)
      vertexStrings = vertexStrings
        .joinWith(incomingAdjacencyListStrings,
          vertexStrings("graphId") === incomingAdjacencyListStrings("graphId") and
            vertexStrings("id") === incomingAdjacencyListStrings("id"),
          "left")
        .map(combineElementStrings)

    } else {

      // 2. union edges with flipped edges and combine strings of parallel edges
      edgeStrings = edgeStrings
        .union(edgeStrings.map(switchSourceTargetIds))
        .groupByKey(e => e.graphId.toString + e.sourceId.toString + e.targetId.toString)
        .flatMapGroups((_, e) => concatElementStrings(e, "&"))

      // 3. extend edge strings by vertex strings
      edgeStrings = edgeStrings
        .joinWith(vertexStrings,
          edgeStrings("graphId") === vertexStrings("graphId") and
          edgeStrings("targetId") === vertexStrings("id"))
        .map(updateTargetString)

      // 4/5. extend vertex strings by vertex+edge strings
      val adjacencyListStrings = edgeStrings
        .groupByKey(e => e.graphId.toString + e.sourceId.toString)
        .flatMapGroups((_, it) => adjacencyList(it, _.sourceId, e => s"\n  -${e.string}-${e.targetString}"))

      // 6. combine vertex strings
      vertexStrings = vertexStrings
        .joinWith(adjacencyListStrings,
          vertexStrings("graphId") === adjacencyListStrings("graphId") and
          vertexStrings("id") === adjacencyListStrings("id"),
          "left")
        .map(combineElementStrings)
    }

    // 7. create adjacency matrix strings
    val adjacencyMatrixStrings = vertexStrings
      .groupByKey(_.graphId)
      .flatMapGroups(adjacencyMatrix)

    // 8. combine graph strings
    graphHeadStrings
      .joinWith(adjacencyMatrixStrings,
        graphHeadStrings("id") === adjacencyMatrixStrings("id"),
        "left")
      .map(combineElementStrings)
  }
}

object CanonicalAdjacencyMatrixBuilder {
  private def switchSourceTargetIds(edgeString: EdgeString): EdgeString = {
    val sourceId = edgeString.sourceId
    edgeString.sourceId = edgeString.targetId
    edgeString.targetId = sourceId
    edgeString
  }

  private def updateSourceString(tuple: Tuple2[EdgeString, VertexString]): EdgeString = {
    tuple._1.sourceString = tuple._2.string
    tuple._1
  }

  private def updateTargetString(tuple: Tuple2[EdgeString, VertexString]): EdgeString = {
    tuple._1.targetString = tuple._2.string
    tuple._1
  }

  private def combineElementStrings[A <: ElementString](tuple: Tuple2[A, A]): A = {
    if (tuple._2 != null) tuple._1.string = tuple._1.string + tuple._2.string
    tuple._1
  }

  private def concatElementStrings[A <: ElementString](values: Iterator[A], sep: String): TraversableOnce[A] = {
    val strings = values.toSeq
    val result = strings.head
    result.string = strings.map(_.string).sorted.mkString(sep)
    Traversable(result)
  }

  private def adjacencyList(edgeStrings: Iterator[EdgeString],
                            idSelector: EdgeString => GradoopId,
                            toString: EdgeString => String): TraversableOnce[VertexString] = {
    val strings = edgeStrings.toSeq
    val first = strings.head
    val string = strings.map(toString).sorted.mkString
    Traversable(VertexString(first.graphId, idSelector(first), string))
  }

  private def adjacencyMatrix(key: GradoopId, vertexStrings: Iterator[VertexString]): TraversableOnce[GraphHeadString] = {
    val strings = vertexStrings.toSeq
    val first = strings.head
    val string = strings.map("\n " + _.string).sorted.mkString
    Traversable(GraphHeadString(first.graphId, string))
  }
}

sealed trait ElementString {
  def string: String
  def string_=(string: String): Unit
}

final case class GraphHeadString(id: GradoopId, var string: String) extends ElementString
final case class VertexString(graphId: GradoopId, id: GradoopId, var string: String) extends ElementString
final case class EdgeString(graphId: GradoopId,
                            var sourceId: GradoopId,
                            var targetId: GradoopId,
                            var sourceString: String,
                            var string: String,
                            var targetString: String) extends ElementString
