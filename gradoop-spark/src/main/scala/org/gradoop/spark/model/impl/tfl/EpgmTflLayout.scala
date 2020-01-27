package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.Dataset
import org.gradoop.spark.model.api.layouts.tfl.TflLayout

class EpgmTflLayout(graphHeads: Map[String, Dataset[L#G]],
  vertices: Map[String, Dataset[L#V]],
  edges: Map[String, Dataset[L#E]],
  graphHeadProperties: Map[String, Dataset[L#P]],
  vertexProperties: Map[String, Dataset[L#P]],
  edgeProperties: Map[String, Dataset[L#P]])
  extends TflLayout[L](graphHeads: Map[String, Dataset[L#G]],
    vertices: Map[String, Dataset[L#V]],
    edges: Map[String, Dataset[L#E]],
    graphHeadProperties: Map[String, Dataset[L#P]],
    vertexProperties: Map[String, Dataset[L#P]],
    edgeProperties: Map[String, Dataset[L#P]])
