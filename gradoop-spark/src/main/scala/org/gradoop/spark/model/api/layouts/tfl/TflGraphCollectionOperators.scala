package org.gradoop.spark.model.api.layouts.tfl

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.operators.changelayout.TflToGve
import org.gradoop.spark.model.impl.operators.setcollection.tfl.{TflDifference, TflIntersection, TflUnion}
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.util.TflFunctions

trait TflGraphCollectionOperators[L <: Tfl[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

  override def union(other: L#GC): L#GC = {
    callForCollection(new TflUnion[L], other)
  }

  override def intersect(other: L#GC): L#GC = {
    callForCollection(new TflIntersection[L], other)
  }

  override def difference(other: L#GC): L#GC = {
    callForCollection(new TflDifference[L], other)
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

  // Tfl only operators

  /** Verifies, if each dataset only contains the correct label.
   *
   * Expensive! Only for debugging!
   *
   * @return this or IllegalStateException
   */
  def verifyLabels: L#GC = {
    factory.init(TflFunctions.verifyLabels(layout.graphHead), TflFunctions.verifyLabels(layout.vertices),
      TflFunctions.verifyLabels(layout.edges), TflFunctions.verifyLabels(layout.graphHeadProperties),
      TflFunctions.verifyLabels(layout.vertexProperties), TflFunctions.verifyLabels(layout.edgeProperties))
  }

  // Change layout

  def asGve[L2 <: Gve[L2]](config: GradoopSparkConfig[L2]): L2#GC = {
    callForValue(new TflToGve[L, L2](config))
  }

  def asTfl[L2 <: Tfl[L2]](config: GradoopSparkConfig[L2]): L2#GC = {
    this.asInstanceOf[L2#GC] // only works, if L2 has the same ModelType
  }
}
