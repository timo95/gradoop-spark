package org.gradoop.spark.model.impl.operators.setcollection.gve

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.operators.BinaryGraphCollectionToGraphCollectionOperator
import org.gradoop.spark.model.impl.types.Gve

class GveUnion[L <: Gve[L]] extends BinaryGraphCollectionToGraphCollectionOperator[L#GC] {

  override def execute(left: L#GC, right: L#GC): L#GC = {
    import left.config.Implicits._

    left.factory.init(left.graphHeads.union(right.graphHeads).dropDuplicates(ColumnNames.ID),
      left.vertices.union(right.vertices).dropDuplicates(ColumnNames.ID),
      left.edges.union(right.edges).dropDuplicates(ColumnNames.ID))
  }
}
