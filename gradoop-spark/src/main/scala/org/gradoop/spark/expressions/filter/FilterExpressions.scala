package org.gradoop.spark.expressions.filter

import org.apache.spark.sql.Column
import org.gradoop.common.util.ColumnNames
import org.apache.spark.sql.functions._

object FilterExpressions {
  val any: Column = lit(true)
  val none: Column = lit(false)

  def hasLabel(label: String): Column = col(ColumnNames.LABEL) === lit(label)

}
