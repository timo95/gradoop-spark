package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.{Edge, Vertex}
import org.gradoop.spark.model.api.graph.ElementAccess

trait Layout[V <: Vertex, E <: Edge] extends Serializable with ElementAccess[V, E] {
}
