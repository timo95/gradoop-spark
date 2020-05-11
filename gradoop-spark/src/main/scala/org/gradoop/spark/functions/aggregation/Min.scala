package org.gradoop.spark.functions.aggregation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.udaf.MinAggregateFunction

class Min(key: String, val name: String) extends AggregationFunction {
  private val minAgg = new MinAggregateFunction

  override def complete(): Column = {
    minAgg(col(ColumnNames.PROPERTIES).getField(key)).as(name)
  }

  override def begin(): Column = {
    minAgg(col(ColumnNames.PROPERTIES).getField(key)).as(name)
  }

  override def finish(): Column = {
    minAgg(col(name)).as(name)
  }
}
