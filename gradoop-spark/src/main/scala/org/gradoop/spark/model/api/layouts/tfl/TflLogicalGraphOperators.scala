package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.expressions.transformation.TransformationFunctions.TransformationFunction
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.operators.changelayout.TflToGve
import org.gradoop.spark.model.impl.operators.setgraph.tfl.TflCombination
import org.gradoop.spark.model.impl.operators.subgraph.tfl.TflSubgraph
import org.gradoop.spark.model.impl.operators.verify.tfl.TflVerify
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

trait TflLogicalGraphOperators[L <: Tfl[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

  // General operators

  override def combine(other: L#LG): L#LG = {
    callForGraph(new TflCombination[L], other)
  }

  override def overlap(other: L#LG): L#LG = {
    throw new RuntimeException("Not implemented")
  }

  override def exclude(other: L#LG): L#LG = {
    throw new RuntimeException("Not implemented")
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

  override def subgraph(vertexFilterExpression: Column, edgeFilterExpression: Column): L#LG = {
    callForGraph(TflSubgraph.both[L](vertexFilterExpression, edgeFilterExpression))
  }

  override def vertexInducedSubgraph(vertexFilterExpression: Column): L#LG = {
    callForGraph(TflSubgraph.vertexInduced[L](vertexFilterExpression))
  }

  override def edgeInducedSubgraph(edgeFilterExpression: Column): L#LG = {
    callForGraph(TflSubgraph.edgeIncuded[L](edgeFilterExpression))
  }

  override def verify: L#LG = callForGraph(new TflVerify[L])

  def transform(graphHeadTransformationFunction: TransformationFunction[L#G],
    vertexTransformationFunction: TransformationFunction[L#V],
    edgeTransformationFunction: TransformationFunction[L#E]): L#LG = {
    throw new RuntimeException("Not implemented")
  }

  def transformGraphHead(graphHeadTransformationFunction: TransformationFunction[L#G]): L#LG = {
    transform(graphHeadTransformationFunction, TransformationFunctions.identity, TransformationFunctions.identity)
  }

  def transformVertices(vertexTransformationFunction: TransformationFunction[L#V]): L#LG = {
    transform(TransformationFunctions.identity, vertexTransformationFunction, TransformationFunctions.identity)
  }

  def transformEdges(edgeTransformationFunction: TransformationFunction[L#E]): L#LG = {
    transform(TransformationFunctions.identity, TransformationFunctions.identity, edgeTransformationFunction)
  }

  // Change layout

  def asGve[L2 <: Gve[L2]](config: GradoopSparkConfig[L2]): L2#LG = {
    callForValue(new TflToGve[L, L2](config))
  }

  def asTfl[L2 <: Tfl[L2]](config: GradoopSparkConfig[L2]): L2#LG = {
    this.asInstanceOf[L2#LG] // only works, if L2 has the same ModelType
  }
}
