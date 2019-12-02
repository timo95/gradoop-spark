package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.gve.{EpgmEdge, EpgmGraphHead, EpgmGveLayout, EpgmGveLayoutFactory, EpgmVertex}

final class EpgmGveLayoutType extends GveLayoutType[EpgmGveLayoutType] with Epgm {
  type T = EpgmGveLayoutType
  type L = EpgmGveLayout
  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge

  type LGF = EpgmGveLayoutFactory[LogicalGraph[T]]
  type GCF = EpgmGveLayoutFactory[GraphCollection[T]]
}
