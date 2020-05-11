package org.gradoop.spark.functions.aggregation.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.gradoop.common.properties.PropertyValueUtils

class MinAggregateFunction extends PropertyValueAggregateFunction {

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftValue = getProp(buffer1)
    val rightValue = getProp(buffer2)

    if(leftValue.nonEmpty && rightValue.nonEmpty) {
      buffer1.update(0, PropertyValueUtils.min(leftValue.get, rightValue.get))
    } else if (rightValue.nonEmpty) {
      buffer1.update(0, rightValue)
    }
  }
}
