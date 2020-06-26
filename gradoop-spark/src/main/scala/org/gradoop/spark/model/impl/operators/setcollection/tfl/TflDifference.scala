package org.gradoop.spark.model.impl.operators.setcollection.tfl

import org.apache.spark.sql.Dataset
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.operators.setcollection.tfl.Functions.removeUncontainedElements
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflDifference[L <: Tfl[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession

    val resGraphHeads = TflFunctions.mergeMapsLeft(left.graphHeads, right.graphHeads,
      (l: Dataset[L#G], r: Dataset[L#G]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftanti").as[L#G].cache())

    val graphIds = TflFunctions.reduceUnion(resGraphHeads.values.map(_.toDF))

    val resVertices = removeUncontainedElements(left.vertices, graphIds)
    val resEdges = removeUncontainedElements(left.edges, graphIds)

    left.factory.init(resGraphHeads, resVertices, resEdges,
      TflFunctions.inducePropMap(resGraphHeads, left.graphHeadProperties),
      TflFunctions.inducePropMap(resVertices, left.vertexProperties),
      TflFunctions.inducePropMap(resEdges, left.edgeProperties))
  }
}
