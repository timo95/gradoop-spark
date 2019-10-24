package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

abstract class GraphCollectionLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection] extends BaseLayoutFactory[G, V, E, LG, GC] {

}
