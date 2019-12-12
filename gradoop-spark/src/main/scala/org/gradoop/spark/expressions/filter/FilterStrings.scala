package org.gradoop.spark.expressions.filter

import org.gradoop.common.util.ColumnNames

object FilterStrings {
  val any = "true"
  val none = "false"

  // TODO use column expressions instead?

  def hasLabel(label: String): String = s"${ColumnNames.LABEL}='$label'"

}
