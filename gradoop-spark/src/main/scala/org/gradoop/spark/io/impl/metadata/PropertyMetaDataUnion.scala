package org.gradoop.spark.io.impl.metadata

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames

/**
 * Aggregation function, that gets properties maps as input and returns the property metadata per group.
 * Used to extract property metadata per label (groupBy(LABEL) beforehand).
 */
class PropertyMetaDataUnion extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField(ColumnNames.PROPERTIES,
    DataTypes.createMapType(StringType,
      StructType(StructField("bytes", DataTypes.BinaryType) :: Nil), false),
    false) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField(ElementMetaData.metaData, dataType, false) :: Nil)

  override def dataType: DataType = DataTypes.createArrayType(StructType(
    StructField(PropertyMetaData.key, StringType, false) ::
      StructField(PropertyMetaData.typeString, StringType, false) :: Nil
  ), false)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array.empty[(String, String)]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val meta = buffer.getSeq[(String, String)](0).toSet
    val newMeta = input.getMap[String, Row](0)
      .mapValues(r => new PropertyValue(r.getAs[Array[Byte]](0)).getExactType.string).toSet
    buffer.update(0, (meta ++ newMeta).toSeq)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val meta = buffer1.getSeq[(String, String)](0).toSet
    val newMeta = buffer2.getSeq[(String, String)](0).toSet
    buffer1.update(0, (meta ++ newMeta).toSeq)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getSeq[(String, String)](0)
  }
}
