package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.layouts.GveLayout

class EpgmGveLayout(graphHeads: Dataset[G], vertices: Dataset[V], edges: Dataset[E])
  extends GveLayout[G, V, E](graphHeads, vertices, edges)