package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.Column

object GroupingUtil {

  def getAlias(column: Column): String = {
    val str = column.toString
    val alias = """(?<=AS `)\w+(?=`$)""".r

    alias.findFirstIn(str) match {
      case Some(alias) => alias
      case None => throw new IllegalArgumentException("Column does not have alias: " + str)
    }
  }
}
