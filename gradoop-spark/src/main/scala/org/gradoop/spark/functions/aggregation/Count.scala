package org.gradoop.spark.functions.aggregation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.gradoop.common.properties.PropertyValue


class Count(val name: String) extends AggregationFunction {
  private val toProp = udf((v: Any) => PropertyValue(v))

  override def complete(): Column = {
    toProp(count("*")).as(name)
  }

  override def begin(): Column = {
    count("*").as(name)
  }

  override def finish(): Column = {
    toProp(sum(col(name))).as(name)
  }
}

object Count {

  def apply(): Count = new Count("count")
}
