package org.gradoop.spark.functions.aggregation

import org.apache.spark.sql.Column

abstract class AggregationFunction {

  val name: String

  def complete(): Column

  def begin(): Column

  def finish(): Column

}
