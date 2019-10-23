package org.gradoop.spark.model.impl.types

import org.gradoop.spark.model.api.types.GraphModel
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}
import org.gradoop.spark.model.impl.graph.{EpgmGraphCollection, EpgmLogicalGraph}

trait EpgmGraphModel extends GraphModel {
  override type G = EpgmGraphHead
  override type V = EpgmVertex
  override type E = EpgmEdge
  override type LG = EpgmLogicalGraph
  override type GC = EpgmGraphCollection
}
