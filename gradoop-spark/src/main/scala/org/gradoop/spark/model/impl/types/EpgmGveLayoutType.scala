package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.impl.gve.{EpgmEdge, EpgmGraphHead, EpgmGveLayout, EpgmVertex}

final class EpgmGveLayoutType extends GveLayoutType with Epgm {
  type L = EpgmGveLayout
  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge
}
