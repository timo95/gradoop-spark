package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.{Edge, Vertex}
import org.gradoop.spark.model.api.graph.ElementAccess
import org.gradoop.spark.model.impl.types.{GveLayoutType, LayoutType}

trait Layout[L <: LayoutType] extends Serializable {

}
