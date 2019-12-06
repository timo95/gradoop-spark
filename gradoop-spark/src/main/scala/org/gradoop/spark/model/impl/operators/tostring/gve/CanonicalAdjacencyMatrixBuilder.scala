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
    else graphStrings
      .groupByKey(_ => "")
      .flatMapGroups((_, g) => concatElementStrings(g, System.lineSeparator))
      .first
      .string
  }

  override def execute(graph: L#LG): String = {
    getGraphStrings(graph.layout, graph.config).first.string
  }

  private def getGraphStrings(gveLayout: L#L, config: GradoopSparkConfig[L]): Dataset[GraphHeadString] = {
    implicit val session: SparkSession = config.sparkSession
    import session.implicits._

    // 1. extract strings representations of the elements
    val graphHeadStrings = gveLayout.graphHeads.map(graphHeadToString)
    val vertexStrings = gveLayout.vertices.flatMap(vertexToString)
    var edgeStrings = gveLayout.edges.flatMap(edgeToString)

    if(directed) {
      // 2. combine labels of parallel edges
      edgeStrings = edgeStrings
        .groupByKey(e => e.graphId.toString + e.sourceId.toString + e.targetId.toString)
        .flatMapGroups((_, e) => concatElementStrings(e, "&"))

      // 3. extend edge labels by vertex labels
      edgeStrings = edgeStrings
        .joinWith(vertexStrings,
          edgeStrings("graphId") === vertexStrings("graphId") and
          edgeStrings("sourceId") === vertexStrings("id"))
        .map(updateSourceString)
        .joinWith(vertexStrings,
          edgeStrings("graphId") === vertexStrings("graphId") and
          edgeStrings("targetId") === vertexStrings("id"))
        .map(updateTargetString)

      // 4. extend vertex labels by outgoing vertex+edge labels
      // 5. extend vertex labels by outgoing vertex+edge labels
      // 6. combine vertex labels
      // TODO ...

    } else {

      // 2. union edges with flipped edges and combine labels of parallel edges
      // 3. extend edge labels by vertex labels
      // 4/5. extend vertex labels by vertex+edge labels
      // 6. combine vertex labels
      // TODO ...

    }

    // 7. create adjacency matrix labels
    // 8. combine graph labels
    // TODO ...

    graphHeadStrings
  }
}

object CanonicalAdjacencyMatrixBuilder {
  private def updateSourceString(tuple: Tuple2[EdgeString, VertexString]): EdgeString = {
    val edgeString = tuple._1
    edgeString.sourceString = tuple._2.string
    edgeString
  }

  private def updateTargetString(tuple: Tuple2[EdgeString, VertexString]): EdgeString = {
    val edgeString = tuple._1
    edgeString.targetString = tuple._2.string
    edgeString
  }

  private def concatElementStrings[A <: ElementString](values: Iterator[A], sep: String): TraversableOnce[A] = {
    val strings = values.toSeq
    val result = strings.head
    result.string = strings.map(_.string).sorted.mkString(sep)
    Traversable(result)
  }
}

sealed trait ElementString {
  def string: String
  def string_=(string: String): Unit
}

final case class GraphHeadString(id: GradoopId, var string: String) extends ElementString
final case class VertexString(graphId: GradoopId, id: GradoopId, var string: String) extends ElementString
final case class EdgeString(graphId: GradoopId,
                            sourceId: GradoopId,
                            targetId: GradoopId,
                            var sourceString: String,
                            var string: String,
                            var targetString: String) extends ElementString
