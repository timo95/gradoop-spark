package org.gradoop.spark.model.impl.operators.setgraph.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.Gve

class GveCombination[L <: Gve[L]] extends BinaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(left: L#LG, right: L#LG): L#LG = {
    import left.config.Implicits._

    left.factory.create(left.vertices.union(right.vertices).dropDuplicates(ColumnNames.ID),
      left.edges.union(right.edges).dropDuplicates(ColumnNames.ID))
  }
}
