package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.Column
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.expressions.transformation.TransformationFunctions.TransformationFunction
import org.gradoop.spark.model.api.graph.LogicalGraphOperators
import org.gradoop.spark.model.impl.types.Tfl

trait TflLogicalGraphOperators[L <: Tfl[L]] extends LogicalGraphOperators[L] {
  this: L#LG =>

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
    throw new RuntimeException("Not implemented")
  }

  override def vertexInducedSubgraph(vertexFilterExpression: Column): L#LG = {
    throw new RuntimeException("Not implemented")
  }

  override def edgeInducedSubgraph(edgeFilterExpression: Column): L#LG = {
    throw new RuntimeException("Not implemented")
  }

  override def verify: L#LG = {
    throw new RuntimeException("Not implemented")
  }

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
}
