package org.gradoop.spark.model.api.layouts.gve

import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.types.Gve

trait GveGraphCollectionOperators[L <: Gve[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

}
