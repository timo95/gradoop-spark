package org.gradoop.spark.model.api.operators

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{BaseGraph, GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait BaseGraphOperators[L <: GveGraphLayout] {
  this: BaseGraph[L] =>

}
