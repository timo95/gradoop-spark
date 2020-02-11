package org.gradoop.spark.expressions

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
    buffer(0) = PropertyValue(0.asInstanceOf[Short])
  }

  override def evaluate(buffer: Row): Any = {
    new PropertyValue(buffer.getAs[Row](0).getAs[Array[Byte]](0))
  }
}
