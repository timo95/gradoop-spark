package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.operators.setcollection.tfl.Functions.removeUncontainedElements
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflIntersection[L <: Tfl[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession

    val leftGraphIds = left.graphHeads.mapValues(_.select(ColumnNames.ID))
    val rightGraphIdsUnion = TflFunctions.reduceUnion(right.graphHeads.values.map(_.select(ColumnNames.ID)))

    val remainingIds = leftGraphIds.mapValues(v => v.intersect(rightGraphIdsUnion))
    val resGraphHeads = left.graphHeads.transform((k, v) =>
      v.join(remainingIds(k), ColumnNames.ID).as[L#G])

    val remainingIdsUnion = TflFunctions.reduceUnion(remainingIds.values)
    val resVertices = removeUncontainedElements(left.vertices, remainingIdsUnion)
    val resEdges = removeUncontainedElements(left.edges, remainingIdsUnion)

    left.factory.init(resGraphHeads, resVertices, resEdges,
      TflFunctions.inducePropMap(resGraphHeads, left.graphHeadProperties),
      TflFunctions.inducePropMap(resVertices, left.vertexProperties),
      TflFunctions.inducePropMap(resEdges, left.edgeProperties))
  }
}
