package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}

trait OldEpgmModel extends OldGraphModel {
  type G = EpgmGraphHead
  type V = EpgmVertex
  type E = EpgmEdge
  //type LG = EpgmLogicalGraph
  //type GC = EpgmGraphCollection
}
