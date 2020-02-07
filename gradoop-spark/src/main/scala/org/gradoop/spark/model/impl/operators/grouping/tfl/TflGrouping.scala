package org.gradoop.spark.model.impl.operators.grouping.tfl

import org.apache.spark.sql.Column
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.types.Tfl

class TflGrouping[L <: Tfl[L]](vertexGroupingKeys: Seq[Column], vertexAggFunctions: Seq[Column],
  edgeGroupingKeys: Seq[Column], edgeAggFunctions: Seq[Column])
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    graph
  }
}

object TflGrouping {

  def apply[L <: Tfl[L]](builder: GroupingBuilder): TflGrouping[L] = {
    new TflGrouping[L](builder.vertexGroupingKeys, builder.vertexAggFunctions,
      builder.edgeGroupingKeys, builder.edgeAggFunctions)
  }
}
