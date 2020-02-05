package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflIntersection[L <: Tfl[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] with TflSetBase {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession
    import sparkSession.implicits._

    val leftGraphIds = left.graphHeads.mapValues(v => v.select(v.id))
    val rightGraphIdsUnion = TflFunctions.reduceUnion(right.graphHeads.values.map(v => v.select(v.id)))

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
