package org.gradoop.spark.util

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.Identifiable
import org.gradoop.spark.functions.filter.FilterExpression

trait Implicits extends Serializable {
  // Wrappers
  implicit def filterExpression(expression: String): FilterExpression = new FilterExpression(expression)
  implicit def columnSelector[T](dataset: Dataset[T]): ColumnSelector[T] = new ColumnSelector[T](dataset)
  implicit def displayConverter[T <: Identifiable](dataset: Dataset[T]): DisplayConverter[T] = new DisplayConverter[T](dataset)
}

object Implicits extends Implicits