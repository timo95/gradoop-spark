package org.gradoop.spark.model.impl.operators.setgraph.tfl

import org.apache.spark.sql.Dataset
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflOverlap[L <: Tfl[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._

    val resVertices = TflFunctions.mergeMapsInner(left.vertices, right.vertices,
      (l: Dataset[L#V], r: Dataset[L#V]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftsemi").as[L#V])
    val resEdges = TflFunctions.mergeMapsInner(left.edges, right.edges,
      (l: Dataset[L#E], r: Dataset[L#E]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftsemi").as[L#E])
    val resVertexProps = TflFunctions.mergeMapsInner(left.vertexProperties, right.vertexProperties,
      (l: Dataset[L#P], r: Dataset[L#P]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftsemi").as[L#P])
    val resEdgeProps = TflFunctions.mergeMapsInner(left.edgeProperties, right.edgeProperties,
      (l: Dataset[L#P], r: Dataset[L#P]) => l.join(r, l(ColumnNames.ID) === r(ColumnNames.ID), "leftsemi").as[L#P])

    factory.init(left.graphHeads, resVertices, resEdges, left.graphHeadProperties, resVertexProps, resEdgeProps)
  }
}
