package org.gradoop.spark.functions.filter

class FilterExpression(val expression: String) extends AnyVal {

  def and(other: String): String = s"(${this.expression}) and (${other})"

  def or(other: String): String = s"(${this.expression}) or (${other})"

  def negate: String = s"not (${this.expression})"
}
