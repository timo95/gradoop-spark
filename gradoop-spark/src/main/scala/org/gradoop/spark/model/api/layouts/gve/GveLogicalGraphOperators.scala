package org.gradoop.spark.model.api.layouts.gve

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.expressions.transformation.TransformationFunctions.TransformationFunction
import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.operators.equality.gve.GveEquals
import org.gradoop.spark.model.impl.operators.subgraph.gve.GveSubgraph
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString
import org.gradoop.spark.model.impl.operators.verify.GveVerify
import org.gradoop.spark.model.impl.types.Gve

trait GveLogicalGraphOperators[L <: Gve[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

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
}
