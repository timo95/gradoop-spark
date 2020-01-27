package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.tfl.{TflGraphCollectionOperators, TflLogicalGraphOperators}
import org.gradoop.spark.model.impl.tfl._

class EpgmTfl extends Tfl[EpgmTfl] with Epgm {
  type T = EpgmTfl
  type L = EpgmTflLayout

  type G = EpgmTflGraphHead
  type V = EpgmTflVertex
  type E = EpgmTflEdge
  type P = EpgmTflProperties

  type LG = LogicalGraph[T] with TflLogicalGraphOperators[T]
  type GC = GraphCollection[T] with TflGraphCollectionOperators[T]

  type LGF = EpgmTflLogicalGraphFactory
  type GCF = EpgmTflGraphCollectionFactory
}
