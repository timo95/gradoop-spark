package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout

final class EpgmGveLayoutType extends GveLayoutType with Epgm {
  type L = EpgmGveLayout
  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge
}
