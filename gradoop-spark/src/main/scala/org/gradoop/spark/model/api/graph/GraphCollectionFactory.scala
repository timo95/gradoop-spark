package org.gradoop.spark.model.api.graph

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.elements.{GraphHead, Vertex}
import org.gradoop.spark.model.api.layouts.LogicalGraphLayoutFactory

class GraphCollectionFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection]
(var layoutFactory: LogicalGraphLayoutFactory[G, V, E, LG, GC]) {

}
