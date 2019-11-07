package org.gradoop.spark.model.impl.operators.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, StructField, StructType}
import org.gradoop.common.model.api.elements.GraphHead
import org.gradoop.common.properties.PropertyValue

class GraphHeadToRow[G <: GraphHead] extends Serializable {

  def getSchema: StructType = {
    val propertyValueType: StructType = StructType(Seq(
      StructField("value", DataTypes.BinaryType, nullable = false),
      StructField("type", DataTypes.StringType, nullable = false)))

    val fields: Seq[StructField] = Seq(
      StructField("graphId", DataTypes.BinaryType, nullable = false),
      StructField("labels", DataTypes.StringType, nullable = false),
      StructField("properties", MapType(DataTypes.StringType, propertyValueType, false), false))

    StructType(fields)
  }

  def call(graphHead: G): Row = {
    new GenericRowWithSchema(Array(graphHead.getId.bytes, graphHead.getLabels,
      graphHead.getProperties.mapValues(v => Tuple2(v.value, v.typeTag))), getSchema)
  }

  def getEncoder: ExpressionEncoder[Row] = {
    RowEncoder(getSchema)
  }
}
