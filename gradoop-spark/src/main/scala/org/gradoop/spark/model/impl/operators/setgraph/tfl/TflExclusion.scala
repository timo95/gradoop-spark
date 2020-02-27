package org.gradoop.spark.model.impl.operators.setgraph.tfl

import org.apache.spark.sql.DataFrame
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflExclusion[L <: Tfl[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._

    // Vertices
    val remainingVertexIds = TflFunctions.mergeMapsLeft(
      left.vertices.mapValues(_.select(ColumnNames.ID)), right.vertices.mapValues(_.select(ColumnNames.ID)),
      (l: DataFrame, r: DataFrame) => l.except(r).distinct)
    val resVertices = TflFunctions.mergeMapsInner(left.vertices.mapValues(_.toDF),
      remainingVertexIds, (l: DataFrame, r: DataFrame) => l.join(r, ColumnNames.ID)).mapValues(_.as[L#V])

    // Edges
    val remainingEdgeIds = TflFunctions.mergeMapsLeft(
      left.edges.mapValues(_.select(ColumnNames.ID)), right.edges.mapValues(_.select(ColumnNames.ID)),
      (l: DataFrame, r: DataFrame) => l.except(r).distinct)
    val resEdges = TflFunctions.mergeMapsInner(left.edges.mapValues(_.toDF),
      remainingEdgeIds, (l: DataFrame, r: DataFrame) => l.join(r, ColumnNames.ID)).mapValues(_.as[L#E])

    // Properties
    val resVertexProps = TflFunctions.inducePropMap(resVertices, left.vertexProperties)
    val resEdgeProps = TflFunctions.inducePropMap(resEdges, left.edgeProperties)

    factory.init(left.graphHeads, resVertices, resEdges, left.graphHeadProperties, resVertexProps, resEdgeProps).removeDanglingEdges
  }
}
