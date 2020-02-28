package org.gradoop.spark.expressions

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, functions}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.udaf.{MaxAggregateFunction, MinAggregateFunction, SumAggregateFunction}

/**
 * Aggregation column expressions, result in property values
 */
object AggregationExpressions {

  private val toProp = udf((v: Any) => PropertyValue(v))

  def count: Column = toProp(functions.count("*")).as("count")

  def sumProp(key: String): Column = {
    val sumAgg = new SumAggregateFunction
    sumAgg(col(ColumnNames.PROPERTIES).getField(key)).as("sum_" + key)
  }

  def minProp(key: String): Column = {
    val minAgg = new MinAggregateFunction
    minAgg(col(ColumnNames.PROPERTIES).getField(key).as("min_" + key))
  }

  def maxProp(key: String): Column = {
    val maxAgg = new MaxAggregateFunction
    maxAgg(col(ColumnNames.PROPERTIES).getField(key).as("max_" + key))
  }
}
