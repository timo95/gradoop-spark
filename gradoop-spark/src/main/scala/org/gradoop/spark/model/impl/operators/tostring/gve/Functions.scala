package org.gradoop.spark.model.impl.operators.tostring.gve

import org.gradoop.common.id.GradoopId
import org.gradoop.spark.model.impl.operators.tostring.gve.CanonicalAdjacencyMatrixBuilder.LINE_SEPARATOR

private[gve] object Functions {

  def switchSourceTargetIds(edgeString: EdgeString): EdgeString = {
    EdgeString(edgeString.graphId, edgeString.targetId, edgeString.sourceId,
      edgeString.sourceString, edgeString.string, edgeString.targetString)
  }

  def concatElementStrings[A <: ElementString](values: Array[A], sep: String): A = {
    val strings = values.toSeq
    val result = strings.head
    result.string = strings.map(_.string).sorted.mkString(sep)
    result
  }

  def adjacencyList(edgeStrings: Array[EdgeString],
    idSelector: EdgeString => GradoopId,
    toString: EdgeString => String): VertexString = {
    val strings = edgeStrings.toSeq
    val first = strings.head
    val string = strings.map(toString).sorted.mkString
    VertexString(first.graphId, idSelector(first), string)
  }

  def adjacencyMatrix(key: GradoopId, vertexStrings: Array[VertexString]): GraphHeadString = {
    val strings = vertexStrings.toSeq
    val first = strings.head
    val string = strings.map(LINE_SEPARATOR + " " + _.string).sorted.mkString
    GraphHeadString(first.graphId, string)
  }
}
