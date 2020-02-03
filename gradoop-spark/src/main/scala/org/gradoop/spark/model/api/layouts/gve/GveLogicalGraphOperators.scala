package org.gradoop.spark.model.api.layouts.gve

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.expressions.transformation.TransformationFunctions.TransformationFunction
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.operators.changelayout.GveToTfl
import org.gradoop.spark.model.impl.operators.equality.gve.GveEquals
import org.gradoop.spark.model.impl.operators.subgraph.gve.GveSubgraph
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString
import org.gradoop.spark.model.impl.operators.verify.gve.GveVerify
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

trait GveLogicalGraphOperators[L <: Gve[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

  // General operators

  override def equalsByElementIds(other: L#LG): Boolean = {
    callForValue(new GveEquals(ElementToString.graphHeadToEmptyString, ElementToString.vertexToIdString,
      ElementToString.edgeToIdString, true), other)
  }

  override def equalsByElementData(other: L#LG): Boolean = {
    callForValue(new GveEquals(ElementToString.graphHeadToEmptyString, ElementToString.vertexToDataString,
      ElementToString.edgeToDataString, true), other)
  }

  override def equalsByData(other: L#LG): Boolean = {
    callForValue(new GveEquals(ElementToString.graphHeadToDataString, ElementToString.vertexToDataString,
      ElementToString.edgeToDataString, true), other)
  }

  override def subgraph(vertexFilterExpression: Column, edgeFilterExpression: Column): L#LG = {
    callForGraph(GveSubgraph.both[L](vertexFilterExpression, edgeFilterExpression))
  }

  override def vertexInducedSubgraph(vertexFilterExpression: Column): L#LG = {
    callForGraph(GveSubgraph.vertexInduced[L](vertexFilterExpression))
  }

  override def edgeInducedSubgraph(edgeFilterExpression: Column): L#LG = {
    callForGraph(GveSubgraph.edgeIncuded[L](edgeFilterExpression))
  }

  override def verify: L#LG = callForGraph(new GveVerify[L])

  def transform(graphHeadTransformationFunction: TransformationFunction[L#G],
                vertexTransformationFunction: TransformationFunction[L#V],
                edgeTransformationFunction: TransformationFunction[L#E]): L#LG = {
    factory.init(graphHeadTransformationFunction(layout.graphHead),
      vertexTransformationFunction(layout.vertices),
      edgeTransformationFunction(layout.edges))
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
    this.asInstanceOf[L2#LG] // only works, if L2 has the same ModelType
  }

  def asTfl[L2 <: Tfl[L2]](config: GradoopSparkConfig[L2]): L2#LG = {
    callForValue(GveToTfl[L, L2](config))
  }
}
