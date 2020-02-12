package org.gradoop.spark.functions
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames

class PropertyKeyFunction(key: String) extends KeyFunction {

  private val nullProp = udf(() => PropertyValue.NULL_VALUE)

  private def nullToProp(column: Column): Column = when(column.isNull, nullProp()).otherwise(column)

  override def name: String = key

  override def extractKey: Column = nullToProp(col(ColumnNames.PROPERTIES).getField(key))

  override def addKey(dataFrame: DataFrame, column: Column): DataFrame = {
    dataFrame.withColumn(ColumnNames.PROPERTIES,
      map_concat(col(ColumnNames.PROPERTIES), map(lit(name), column)))
  }
}
