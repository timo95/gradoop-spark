package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.impl.gve._

final class EpgmGveLayoutType extends GveLayoutType[EpgmGveLayoutType] with Epgm {
  type T = EpgmGveLayoutType
  type L = EpgmGveLayout
  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge

  type LGF = EpgmGveLogicalGraphFactory
  type GCF = EpgmGveGraphCollectionFactory
}
