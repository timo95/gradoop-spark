package org.gradoop.spark.model.api.operators

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

trait GraphCollectionOperators[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph[G, V, E, LG, GC], GC <: GraphCollection[G, V, E, LG, GC]] {
  this: GraphCollection[G, V, E, LG, GC] =>

}
