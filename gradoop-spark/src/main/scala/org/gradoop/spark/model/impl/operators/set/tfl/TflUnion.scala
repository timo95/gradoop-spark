package org.gradoop.spark.model.impl.operators.set.tfl

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflUnion[L <: Tfl[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    val factory = left.factory
    import factory.Implicits._
    implicit val sparkSession = factory.sparkSession

    val resGraphHeads = TflFunctions.unionMaps(left.graphHeads, right.graphHeads)
      .mapValues(_.dropDuplicates(ColumnNames.ID))
    val resVertices = TflFunctions.unionMaps(left.vertices, right.vertices)
      .mapValues(_.dropDuplicates(ColumnNames.ID))
    val resEdges = TflFunctions.unionMaps(left.edges, right.edges)
      .mapValues(_.dropDuplicates(ColumnNames.ID))
    val resGraphHeadProps = TflFunctions.unionMaps(left.graphHeadProperties,
      right.graphHeadProperties).mapValues(_.dropDuplicates(ColumnNames.ID))
    val resVertexProps = TflFunctions.unionMaps(left.vertexProperties,
      right.vertexProperties).mapValues(_.dropDuplicates(ColumnNames.ID))
    val resEdgeProps = TflFunctions.unionMaps(left.edgeProperties,
      right.edgeProperties).mapValues(_.dropDuplicates(ColumnNames.ID))

    left.factory.init(resGraphHeads, resVertices, resEdges, resGraphHeadProps, resVertexProps, resEdgeProps)
  }
}
