package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators._
import org.gradoop.spark.model.impl.types.{Gve, LayoutType, Tfl}

trait GraphCollectionOperators[L <: LayoutType[L]] {
  this: L#GC =>

  def union(other: L#GC): L#GC

  def intersect(other: L#GC): L#GC

  def difference(other: L#GC): L#GC

  def equalsByGraphIds(other: L#GC): Boolean

  def equalsByGraphElementIds(other: L#GC): Boolean

  def equalsByGraphElementData(other: L#GC): Boolean

  def equalsByGraphData(other: L#GC): Boolean

  // Change layout

  def asGve[L2 <: Gve[L2]](config: GradoopSparkConfig[L2]): L2#GC

  def asTfl[L2 <: Tfl[L2]](config: GradoopSparkConfig[L2]): L2#GC

  // Call for operators

  def callForValue[V](operator: UnaryGraphCollectionToValueOperator[L#GC, V]): V = {
    operator.execute(this)
  }

  def callForValue[V](operator: BinaryGraphCollectionToValueOperator[L#GC, V], other: L#GC): V = {
    operator.execute(this, other)
  }

  def callForCollection(operator: BinaryGraphCollectionToGraphCollectionOperator[L#GC], other: L#GC): L#GC = {
    callForValue(operator, other)
  }
}
