package org.gradoop.spark.util

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.components.Identifiable
import org.gradoop.spark.functions.filter.FilterExpression
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.GveLayout
import org.gradoop.spark.model.impl.types.GveLayoutType

trait Implicits extends Serializable {
  // Wrappers
  implicit def filterExpression(expression: String): FilterExpression = new FilterExpression(expression)
  implicit def columnSelector[T](dataset: Dataset[T]): ColumnSelector[T] = new ColumnSelector[T](dataset)
  implicit def displayConverter[T <: Identifiable](dataset: Dataset[T]): DisplayConverter[T] = new DisplayConverter[T](dataset)

  implicit def getGveLayout[L <: GveLayoutType](graph: BaseGraph[L]): GveLayout[L] = graph.layout
}

object Implicits extends Implicits