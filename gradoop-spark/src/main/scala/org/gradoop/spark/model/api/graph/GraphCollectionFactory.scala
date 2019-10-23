package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.layouts.LogicalGraphLayoutFactory

class GraphCollectionFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection]
(var layoutFactory: LogicalGraphLayoutFactory[G, V, E, LG, GC]) {

}
