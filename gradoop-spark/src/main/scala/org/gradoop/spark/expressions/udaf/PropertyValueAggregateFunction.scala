package org.gradoop.spark.expressions.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.gradoop.common.properties.PropertyValue

abstract class PropertyValueAggregateFunction extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", dataType) :: Nil)

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = StructType(StructField("bytes", DataTypes.BinaryType) :: Nil)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = merge(buffer, input)

  override def evaluate(buffer: Row): Any = {
    getProp(buffer).getOrElse(PropertyValue.NULL_VALUE)
  }

  protected def getProp(input: Row): Option[PropertyValue] = {
    val value = input.getAs[Row](0)
    if(value == null) None
    else Some(new PropertyValue(value.getAs[Array[Byte]](0)))
  }
}
