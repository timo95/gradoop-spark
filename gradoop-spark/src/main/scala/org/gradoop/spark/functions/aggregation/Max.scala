package org.gradoop.spark.functions.aggregation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.functions.aggregation.udaf.MaxAggregateFunction

class Max(key: String, val name: String) extends AggregationFunction {
  private val maxAgg = new MaxAggregateFunction

  override def complete(): Column = {
    maxAgg(col(ColumnNames.PROPERTIES).getField(key)).as(name)
  }

  override def begin(): Column = {
    maxAgg(col(ColumnNames.PROPERTIES).getField(key)).as(name)
  }

  override def finish(): Column = {
    maxAgg(col(name)).as(name)
  }
}

object Max {

  def apply(key: String): Max = new Max(key, "max_" + key)
}
