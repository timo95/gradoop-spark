package org.gradoop.spark.model.impl.operators.equality.gve

import org.gradoop.spark.model.api.operators.{BinaryGraphCollectionToValueOperator, BinaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.operators.tostring.gve.{CanonicalAdjacencyMatrixBuilder, EdgeString, ElementToString, GraphHeadString, VertexString}
import org.gradoop.spark.model.impl.types.Gve

import scala.collection.TraversableOnce

class GveEquals[L <: Gve[L]](graphHeadToString: L#G => GraphHeadString,
                             vertexToString: L#V => TraversableOnce[VertexString],
                             edgeToString: L#E => TraversableOnce[EdgeString],
                             directed: Boolean)
  extends BinaryLogicalGraphToValueOperator[L#LG, Boolean]
    with BinaryGraphCollectionToValueOperator[L#GC, Boolean] {
  val matrixBuilder = new CanonicalAdjacencyMatrixBuilder[L](graphHeadToString, vertexToString, edgeToString, directed)

  override def execute(left: L#LG, right: L#LG): Boolean = {
    matrixBuilder.execute(left) equals matrixBuilder.execute(right)
  }

  override def execute(left: L#GC, right: L#GC): Boolean = {
    matrixBuilder.execute(left) equals matrixBuilder.execute(right)
  }
}
