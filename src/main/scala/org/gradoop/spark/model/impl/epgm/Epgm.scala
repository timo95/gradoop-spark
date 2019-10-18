package org.gradoop.spark.model.impl.epgm

import org.gradoop.common.model.impl.pojo.{EPGMEdge, EPGMGraphHead, EPGMVertex}
import org.gradoop.spark.model.api.GraphModel
import org.gradoop.spark.model.impl.epgm.graph.{EpgmGraphCollection, EpgmLogicalGraph}

trait Epgm extends GraphModel {
  type G = EPGMGraphHead
  type V = EPGMVertex
  type E = EPGMEdge
  type LG = EpgmLogicalGraph
  type GC = EpgmGraphCollection
}
