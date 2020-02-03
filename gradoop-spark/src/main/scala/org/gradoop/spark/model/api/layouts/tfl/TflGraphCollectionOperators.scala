package org.gradoop.spark.model.api.layouts.tfl

import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.types.Tfl

trait TflGraphCollectionOperators[L <: Tfl[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

  override def difference(other: L#GC): L#GC = {
    throw new RuntimeException("Not implemented")
  }

  override def equalsByGraphIds(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  override def equalsByGraphElementIds(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  override def equalsByGraphElementData(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  override def equalsByGraphData(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }
}
