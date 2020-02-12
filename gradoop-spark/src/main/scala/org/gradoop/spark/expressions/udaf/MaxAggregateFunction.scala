package org.gradoop.spark.expressions.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.gradoop.common.properties.PropertyValueUtils

class MaxAggregateFunction extends PropertyValueAggregateFunction {

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftValue = getProp(buffer1)
    val rightValue = getProp(buffer2)

    if(leftValue.nonEmpty && rightValue.nonEmpty) {
      buffer1.update(0, PropertyValueUtils.max(leftValue.get, rightValue.get))
    } else if (rightValue.nonEmpty) {
      buffer1.update(0, rightValue)
    }
  }
}
