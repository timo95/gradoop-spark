package org.gradoop.spark.expressions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row, functions}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames

/**
 * Aggregation column expressions
 */
object AggregateExpressions {
/*
  private val getNumber = udf((bytes: Row) => {
    new PropertyValue(bytes(0).asInstanceOf[Array[Byte]]).get // TODO not supported in UDFs
  })*/

  def count: Column = functions.count("*").as("count")

  // PropertyValue aggregate functions (supports numeric property types)
  /*def sumProp(key: String): Column = {
    sum(getNumber(col(ColumnNames.PROPERTIES).getField(key))).as("sum(%s)".format(key))
  }*/ // TODO implement my own sum, etc aggregation funcs or make specific ones for each type
}
