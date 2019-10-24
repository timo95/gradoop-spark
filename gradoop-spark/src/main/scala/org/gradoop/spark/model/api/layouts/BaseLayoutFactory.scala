package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.{Edge, ElementFactoryProvider, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

abstract class BaseLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection]
  extends ElementFactoryProvider[G, V, E] {

}
