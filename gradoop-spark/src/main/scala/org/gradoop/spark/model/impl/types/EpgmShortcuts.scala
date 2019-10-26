package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}
import org.gradoop.spark.model.impl.graph.{EpgmGraphCollection, EpgmLogicalGraph}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout

trait EpgmShortcuts {
  // Types
  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge
  type LG = EpgmLogicalGraph
  type GC = EpgmGraphCollection

  // Objects
  val GVE: EpgmGveLayout.type = EpgmGveLayout
}
