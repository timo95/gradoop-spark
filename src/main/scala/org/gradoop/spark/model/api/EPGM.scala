package org.gradoop.spark.model.api

import org.gradoop.common.model.impl.pojo.{EPGMEdge, EPGMGraphHead, EPGMVertex}

trait EPGM {
  type G = EPGMGraphHead
  type V = EPGMVertex
  type E = EPGMEdge
}
