package org.gradoop.spark.functions.aggregation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.functions.aggregation.udaf.MinAggregateFunction

class Min(key: String, val name: String) extends AggregationFunction {
  private val minAgg = new MinAggregateFunction

  override def aggregate(): Column = {
    minAgg(col(ColumnNames.PROPERTIES).getField(key)).as(name)
  }

}

object Min {

  def apply(key: String): Min = new Min(key, "min_" + key)
}
