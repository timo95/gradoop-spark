package org.gradoop.spark.functions.aggregation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.functions.aggregation.udaf.SumAggregateFunction

class Sum(key: String, val name: String) extends AggregationFunction {
  private val sumAgg = new SumAggregateFunction

  override def aggregate(): Column = {
    sumAgg(col(ColumnNames.PROPERTIES).getField(key)).as(name)
  }

}

object Sum {

  def apply(key: String): Sum = new Sum(key, "sum_" + key)
}
