package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.gradoop.common.id.GradoopId
import org.gradoop.spark.expressions.AggregateExpressions

object GroupingUtil {

  // Column Constants
  val KEYS = "groupingKeys"
  val SUPER_ID = "superId"
  val VERTEX_ID = "vertexId"

  // Default aggregation, used if agg is empty
  val defaultAgg: Column = AggregateExpressions.count

  // UDFs
  val longToId: UserDefinedFunction = udf(long => GradoopId.fromLong(long)) // Use this with spark ids
  val emptyIdSet: UserDefinedFunction = udf(() => Array.empty[GradoopId])

  def getAlias(column: Column): String = {
    val regex = """(?<=AS `)[^`]+(?=`$)""".r
    regex.findFirstIn(column.toString) match {
      case Some(alias) => alias
      case None => throw new IllegalArgumentException("Column does not have alias: " + column.toString)
    }
  }
}
