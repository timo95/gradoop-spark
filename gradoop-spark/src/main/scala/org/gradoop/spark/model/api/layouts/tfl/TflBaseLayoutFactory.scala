package org.gradoop.spark.model.api.layouts.tfl

import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.Tfl

trait TflBaseLayoutFactory[L <: Tfl[L], BG <: BaseGraph[L]] extends LogicalGraphLayoutFactory[L]
  with GraphCollectionLayoutFactory[L] {

}
