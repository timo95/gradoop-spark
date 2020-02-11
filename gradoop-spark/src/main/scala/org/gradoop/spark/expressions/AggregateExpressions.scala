package org.gradoop.spark.expressions

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, functions}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames

/**
 * Aggregation column expressions, returns property value
 */
object AggregateExpressions {

  private val toProp = udf((v: Any) => PropertyValue(v))

  def count: Column = toProp(functions.count("*")).as("count")

  def sumProp(key: String): Column = {
    val sumAgg = new SumAggregateFunction
    sumAgg(col(ColumnNames.PROPERTIES).getField(key)).as("sum_" + key)
  }
}
