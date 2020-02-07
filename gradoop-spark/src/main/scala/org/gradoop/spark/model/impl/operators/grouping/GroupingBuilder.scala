package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.Column

class GroupingBuilder {

  var vertexGroupingKeys: Seq[Column] = Seq.empty

  var vertexAggFunctions: Seq[Column] = Seq.empty

  var edgeGroupingKeys: Seq[Column] = Seq.empty

  var edgeAggFunctions: Seq[Column] = Seq.empty
}
