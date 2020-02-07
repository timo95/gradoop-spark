package org.gradoop.spark.util

import org.apache.spark.sql.{DataFrame, Dataset}
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.gve.GveLayout
import org.gradoop.spark.model.api.layouts.tfl.TflLayout
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

trait Implicits extends Serializable {
  // Wrappers
  implicit def columnSelector[T](dataset: Dataset[T]): ColumnSelector[T] = new ColumnSelector[T](dataset)
  implicit def displayConverter(dataset: Dataset[_]): DisplayConverter = new DisplayConverter(dataset.toDF)
  implicit def displayConverterDF(dataFrame: DataFrame): DisplayConverter = new DisplayConverter(dataFrame)

  // Get Layout
  implicit def getGveLayout[L <: Gve[L]](graph: BaseGraph[L]): GveLayout[L] = graph.layout
  implicit def getTflLayout[L <: Tfl[L]](graph: BaseGraph[L]): TflLayout[L] = graph.layout
}

object Implicits extends Implicits
