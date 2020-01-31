package org.gradoop.spark.expressions.filter

import org.apache.spark.sql.Column
import org.gradoop.common.util.ColumnNames
import org.apache.spark.sql.functions._
import org.gradoop.common.properties.PropertyValue

object FilterExpressions {
  val any: Column = lit(true)
  val none: Column = lit(false)

  def hasLabel(label: String): Column = col(ColumnNames.LABEL) === lit(label)

  def hasProperty(key: String): Column = map_keys(col(ColumnNames.PROPERTIES)).contains(lit(key))

  def hasProperty(key: String, value: PropertyValue): Column = {
    col(ColumnNames.PROPERTIES).getField(key).getItem(PropertyValue.bytes) === lit(value.bytes)
  }
}
