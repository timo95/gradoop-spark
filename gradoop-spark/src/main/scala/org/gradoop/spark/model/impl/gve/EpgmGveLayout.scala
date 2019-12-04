package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.layouts._
import org.gradoop.spark.model.api.layouts.gve.GveLayout

class EpgmGveLayout(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E])
  extends GveLayout[L](graphHeads, vertices, edges)
