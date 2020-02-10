package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.Column
import org.gradoop.spark.functions.KeyFunction

class GroupingBuilder {

  var vertexGroupingKeys: Seq[KeyFunction] = Seq.empty

  var vertexAggFunctions: Seq[Column] = Seq.empty

  var edgeGroupingKeys: Seq[KeyFunction] = Seq.empty

  var edgeAggFunctions: Seq[Column] = Seq.empty
}
