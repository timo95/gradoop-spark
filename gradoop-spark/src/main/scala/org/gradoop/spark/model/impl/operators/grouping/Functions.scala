package org.gradoop.spark.model.impl.operators.grouping

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.gradoop.common.id.GradoopId
import org.gradoop.spark.functions.aggregation.{AggregationFunction, Count}

private[grouping] object Functions {

  // Column Constants
  val KEYS = "groupingKeys"
  val SUPER_ID = "superId"
  val VERTEX_ID = "vertexId"

  // Default aggregation, if agg is empty
  val DEFAULT_AGG: AggregationFunction = new Count("count")

  // UDFs
  val longToId: UserDefinedFunction = udf(long => GradoopId.fromLong(long)) // Use with monotonically_increasing_id()
}
