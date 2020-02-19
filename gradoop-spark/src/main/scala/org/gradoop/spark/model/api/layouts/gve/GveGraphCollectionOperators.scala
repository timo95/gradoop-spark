package org.gradoop.spark.model.api.layouts.gve

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.operators.changelayout.GveToTfl
import org.gradoop.spark.model.impl.operators.equality.gve.{GveEquals, GveGraphCollectionEqualityByGraphIds}
import org.gradoop.spark.model.impl.operators.setcollection.gve.{GveDifference, GveIntersection, GveUnion}
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.transformation.TransformationFunctions
import org.gradoop.spark.transformation.TransformationFunctions.TransformationFunction

trait GveGraphCollectionOperators[L <: Gve[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

  override def union(other: L#GC): L#GC = {
    callForCollection(new GveUnion[L], other)
  }

  override def intersect(other: L#GC): L#GC = {
    callForCollection(new GveIntersection[L], other)
  }

  override def difference(other: L#GC): L#GC = {
    callForCollection(new GveDifference[L], other)
  }

  override def equalsByGraphIds(other: L#GC): Boolean = {
    callForValue(new GveGraphCollectionEqualityByGraphIds, other)
  }

  override def equalsByGraphElementIds(other: L#GC): Boolean = {
    callForValue(new GveEquals(ElementToString.graphHeadToEmptyString, ElementToString.vertexToIdString,
      ElementToString.edgeToIdString, true), other)
  }

  override def equalsByGraphElementData(other: L#GC): Boolean = {
    callForValue(new GveEquals(ElementToString.graphHeadToEmptyString, ElementToString.vertexToDataString,
      ElementToString.edgeToDataString, true), other)
  }

  override def equalsByGraphData(other: L#GC): Boolean = {
    callForValue(new GveEquals(ElementToString.graphHeadToDataString, ElementToString.vertexToDataString,
      ElementToString.edgeToDataString, true), other)
  }

  def transform(graphHeadTransformationFunction: TransformationFunction[L#G],
    vertexTransformationFunction: TransformationFunction[L#V],
    edgeTransformationFunction: TransformationFunction[L#E]): L#GC = {
    factory.init(graphHeadTransformationFunction(layout.graphHead),
      vertexTransformationFunction(layout.vertices),
      edgeTransformationFunction(layout.edges))
  }

  def transformGraphHead(graphHeadTransformationFunction: TransformationFunction[L#G]): L#GC = {
    transform(graphHeadTransformationFunction, TransformationFunctions.identity, TransformationFunctions.identity)
  }

  def transformVertices(vertexTransformationFunction: TransformationFunction[L#V]): L#GC = {
    transform(TransformationFunctions.identity, vertexTransformationFunction, TransformationFunctions.identity)
  }

  def transformEdges(edgeTransformationFunction: TransformationFunction[L#E]): L#GC = {
    transform(TransformationFunctions.identity, TransformationFunctions.identity, edgeTransformationFunction)
  }

  // Change layout

  def asGve[L2 <: Gve[L2]](config: GradoopSparkConfig[L2]): L2#GC = {
    this.asInstanceOf[L2#GC] // only works, if L2 has the same ModelType
  }

  def asTfl[L2 <: Tfl[L2]](config: GradoopSparkConfig[L2]): L2#GC = {
    callForValue(GveToTfl[L, L2](config))
  }
}
