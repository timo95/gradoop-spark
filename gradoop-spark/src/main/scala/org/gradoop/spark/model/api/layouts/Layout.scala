package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.{Edge, Vertex}
import org.gradoop.spark.model.api.graph.ElementAccess
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait Layout[L <: GveGraphLayout] extends ElementAccess[L#V, L#E] with Serializable {

}
