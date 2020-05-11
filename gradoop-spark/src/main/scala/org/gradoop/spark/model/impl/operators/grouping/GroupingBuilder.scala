package org.gradoop.spark.model.impl.operators.grouping

import org.gradoop.spark.functions.KeyFunction
import org.gradoop.spark.functions.aggregation.AggregationFunction

class GroupingBuilder {

  var vertexGroupingKeys: Seq[KeyFunction] = Seq.empty

  var vertexAggFunctions: Seq[AggregationFunction] = Seq.empty

  var edgeGroupingKeys: Seq[KeyFunction] = Seq.empty

  var edgeAggFunctions: Seq[AggregationFunction] = Seq.empty
}
