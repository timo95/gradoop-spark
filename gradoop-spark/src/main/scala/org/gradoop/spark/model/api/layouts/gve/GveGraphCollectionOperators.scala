package org.gradoop.spark.model.api.layouts.gve

import org.gradoop.spark.model.api.graph.GraphCollectionOperators
import org.gradoop.spark.model.impl.operators.difference.GveDifference
import org.gradoop.spark.model.impl.operators.equality.gve.{GveEquals, GveGraphCollectionEqualityByGraphIds}
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString
import org.gradoop.spark.model.impl.types.Gve

trait GveGraphCollectionOperators[L <: Gve[L]] extends GraphCollectionOperators[L] {
  this: L#GC =>

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
}
