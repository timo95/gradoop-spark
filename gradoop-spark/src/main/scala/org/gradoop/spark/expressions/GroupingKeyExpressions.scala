package org.gradoop.spark.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.gradoop.common.util.ColumnNames

/**
 * Selection column expressions
 */
object GroupingKeyExpressions {

  val label: Column = col(ColumnNames.LABEL).as(":label")

  def property(key: String): Column = col(ColumnNames.PROPERTIES).getField(key).as(key)
}
