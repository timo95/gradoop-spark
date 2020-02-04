package org.gradoop.spark.model.impl.operators.setgraph.tfl

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions

class TflCombination[L <: Tfl[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    val factory = left.factory
    import factory.Implicits._

    factory.create(TflFunctions.unionMaps(left.vertices, right.vertices).mapValues(_.dropDuplicates(ColumnNames.ID)),
      TflFunctions.unionMaps(left.edges, right.edges).mapValues(_.dropDuplicates(ColumnNames.ID)),
      TflFunctions.unionMaps(left.vertexProperties, right.vertexProperties).mapValues(_.dropDuplicates(ColumnNames.ID)),
      TflFunctions.unionMaps(left.edgeProperties, right.edgeProperties).mapValues(_.dropDuplicates(ColumnNames.ID))
    )
  }
}
