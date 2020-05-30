package org.gradoop.spark.model.impl.operators.grouping

import org.gradoop.spark.functions.KeyFunction
import org.gradoop.spark.functions.aggregation.AggregationFunction

class GroupingBuilder {

  var vertexGroupingKeys: Seq[KeyFunction] = Seq.empty

  var vertexAggFunctions: Seq[AggregationFunction] = Seq.empty

  var edgeGroupingKeys: Seq[KeyFunction] = Seq.empty

  var edgeAggFunctions: Seq[AggregationFunction] = Seq.empty

  override def toString: String = {
    val separator = ", "

    s"""Vertex Grouping Keys: ${vertexGroupingKeys.map(_.name).mkString(separator)}
      |Vertex Aggregation Functions: ${vertexAggFunctions.map(_.name).mkString(separator)}
      |Edge Grouping Keys: ${edgeGroupingKeys.map(_.name).mkString(separator)}
      |Edge Aggregation Functions: ${edgeAggFunctions.map(_.name).mkString(separator)}""".stripMargin
  }
}
