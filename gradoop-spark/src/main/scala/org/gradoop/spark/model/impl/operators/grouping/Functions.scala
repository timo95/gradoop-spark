package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.gradoop.common.id.GradoopId
import org.gradoop.spark.expressions.AggregationExpressions

private[grouping] object Functions {

  // Column Constants
  val KEYS = "groupingKeys"
  val SUPER_ID = "superId"
  val VERTEX_ID = "vertexId"

  // Default aggregation, if agg is empty
  val DEFAULT_AGG: Column = AggregationExpressions.count

  // UDFs
  val longToId: UserDefinedFunction = udf(long => GradoopId.fromLong(long)) // Use with monotonically_increasing_id()

  def getAlias(column: Column): String = {
    val regex = """(?<=AS `)[^`]+(?=`$)""".r
    regex.findFirstIn(column.toString) match {
      case Some(alias) => alias
      case None => throw new IllegalArgumentException("Column does not have alias: " + column.toString)
    }
  }
}
