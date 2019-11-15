package org.gradoop.spark.functions.filter

import org.gradoop.common.util.ColumnNames

object FilterStrings {
  val any = "true"
  val none = "false"

  // TODO use column expressions instead?

  def hasLabel(label: String) = s"${ColumnNames.LABEL}='$label'"

}
