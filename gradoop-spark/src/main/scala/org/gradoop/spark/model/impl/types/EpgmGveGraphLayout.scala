package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}

final class EpgmGveGraphLayout extends GveGraphLayout with Epgm {
  type M = Epgm

  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge
}
