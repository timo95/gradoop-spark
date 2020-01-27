package org.gradoop.spark.model.api.layouts.tfl

import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.types.Tfl

trait TflGraphCollectionOperators[L <: Tfl[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

  def equalsByGraphIds(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  def equalsByGraphElementIds(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  def equalsByGraphElementData(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  def equalsByGraphData(other: L#GC): Boolean = {
    throw new RuntimeException("Not implemented")
  }
}
