package org.gradoop.spark.model.impl.operators.setgraph.tfl

import org.apache.spark.sql.Dataset
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflExclusion[L <: Tfl[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._

    val vertices = TflFunctions.mergeMapsLeft(left.vertices, right.vertices,
      (l: Dataset[L#V], r: Dataset[L#V]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftanti").as[L#V])
    val edges = TflFunctions.mergeMapsLeft(left.edges, right.edges,
      (l: Dataset[L#E], r: Dataset[L#E]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftanti").as[L#E])
    val vertexProps = TflFunctions.mergeMapsLeft(left.vertexProperties, right.vertexProperties,
      (l: Dataset[L#P], r: Dataset[L#P]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftanti").as[L#P])
    val edgeProps = TflFunctions.mergeMapsLeft(left.edgeProperties, right.edgeProperties,
      (l: Dataset[L#P], r: Dataset[L#P]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftanti").as[L#P])

    factory.init(left.graphHeads, vertices, edges, left.graphHeadProperties, vertexProps, edgeProps).removeDanglingEdges
  }
}
