package org.gradoop.spark.functions.filter

import org.gradoop.common.util.ColumnNames

class FilterExpression(val expression: String) extends AnyVal {

  def and(other: String): String = s"(${this.expression}) and (${other})"

  def or(other: String): String = s"(${this.expression}) or (${other})"

  def negate: String = s"not (${this.expression})"
}

object FilterExpression {
  val any = "true"
  val none = "false"

  // TODO use column expressions instead?

  def hasLabel(label: String) = s"${ColumnNames.LABELS}='$label'"

}
