package org.gradoop.spark.expressions

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.gradoop.common.properties.{PropertyValue, PropertyValueUtils}

class SumAggregateFunction extends PropertyValueAggregateFunction {

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val oldValue = new PropertyValue(buffer.getAs[Row](0).getAs[Array[Byte]](0))
    val newValue = new PropertyValue(input.getAs[Row](0).getAs[Array[Byte]](0))
    buffer.update(0, PropertyValueUtils.add(oldValue, newValue))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)
}
