package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.Column
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.operators.changelayout.TflToGve
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.operators.grouping.tfl.TflGrouping
import org.gradoop.spark.model.impl.operators.setgraph.tfl.{TflCombination, TflExclusion, TflOverlap}
import org.gradoop.spark.model.impl.operators.subgraph.tfl.TflSubgraph
import org.gradoop.spark.model.impl.operators.verify.tfl.TflRemoveDanglingEdges
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.util.TflFunctions

trait TflLogicalGraphOperators[L <: Tfl[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

  // General operators

  override def combine(other: L#LG): L#LG = {
    callForGraph(new TflCombination[L], other)
  }

  override def overlap(other: L#LG): L#LG = {
    callForGraph(new TflOverlap[L], other)
  }

  override def exclude(other: L#LG): L#LG = {
    callForGraph(new TflExclusion[L], other)
  }

  override def equalsByElementIds(other: L#LG): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  override def equalsByElementData(other: L#LG): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  override def equalsByData(other: L#LG): Boolean = {
    throw new RuntimeException("Not implemented")
  }

  override def groupBy(builder: GroupingBuilder): L#LG = {
    callForGraph(TflGrouping[L](builder))
  }

  override def subgraph(vertexFilterExpression: Column, edgeFilterExpression: Column): L#LG = {
    callForGraph(TflSubgraph.both[L](vertexFilterExpression, edgeFilterExpression))
  }

  override def vertexInducedSubgraph(vertexFilterExpression: Column): L#LG = {
    callForGraph(TflSubgraph.vertexInduced[L](vertexFilterExpression))
  }

  override def edgeInducedSubgraph(edgeFilterExpression: Column): L#LG = {
    callForGraph(TflSubgraph.edgeIncuded[L](edgeFilterExpression))
  }

  override def removeDanglingEdges: L#LG = callForGraph(new TflRemoveDanglingEdges[L])

  override def cache: L#LG = {
    factory.init(layout.graphHeads.mapValues(_.cache), layout.vertices.mapValues(_.cache),
      layout.edges.mapValues(_.cache), layout.graphHeadProperties.mapValues(_.cache),
      layout.vertexProperties.mapValues(_.cache), layout.edgeProperties.mapValues(_.cache))
  }

  // Tfl only operators

  /** Verifies, if each dataset only contains the correct label.
   *
   * Expensive! Only for debugging!
   *
   * @return this or IllegalStateException
   */
  def verifyLabels: L#LG = {
    factory.init(TflFunctions.verifyLabels(layout.graphHead), TflFunctions.verifyLabels(layout.vertices),
      TflFunctions.verifyLabels(layout.edges), TflFunctions.verifyLabels(layout.graphHeadProperties),
      TflFunctions.verifyLabels(layout.vertexProperties), TflFunctions.verifyLabels(layout.edgeProperties))
  }

  // Change layout

  def asGve[L2 <: Gve[L2]](config: GradoopSparkConfig[L2]): L2#LG = {
    callForValue(new TflToGve[L, L2](config))
  }

  def asTfl[L2 <: Tfl[L2]](config: GradoopSparkConfig[L2]): L2#LG = {
    this.asInstanceOf[L2#LG] // only works, if L2 has the same ModelType
  }
}
