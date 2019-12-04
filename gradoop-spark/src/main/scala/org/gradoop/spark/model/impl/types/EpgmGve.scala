package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.gve.{GveGraphCollectionOperators, GveLogicalGraphOperators}
import org.gradoop.spark.model.impl.gve._

final class EpgmGve extends Gve[EpgmGve] with Epgm {
  type T = EpgmGve
  type L = EpgmGveLayout
  type G = EpgmGveGraphHead
  type V = EpgmGveVertex
  type E = EpgmGveEdgeFactory

  type LG = LogicalGraph[T] with GveLogicalGraphOperators[T]
  type GC = GraphCollection[T] with GveGraphCollectionOperators[T]

  type LGF = EpgmGveLogicalGraphFactory
  type GCF = EpgmGveGraphCollectionFactory
}
