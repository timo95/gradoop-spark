package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.Column

object GroupingUtil {

  def getAlias(column: Column): String = {
    val regex = """(?<=AS `)[^`]+(?=`$)""".r
    regex.findFirstIn(column.toString) match {
      case Some(alias) => alias
      case None => throw new IllegalArgumentException("Column does not have alias: " + column.toString)
    }
  }
}
