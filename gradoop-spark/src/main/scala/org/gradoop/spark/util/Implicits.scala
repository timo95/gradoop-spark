package org.gradoop.spark.util

import org.apache.spark.sql.{DataFrame, Dataset}
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.gve.GveLayout
import org.gradoop.spark.model.api.layouts.tfl.TflLayout
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

trait Implicits extends ImplicitTypedColumnSelector {

  // Wrappers
  implicit def readableWrapper(dataset: Dataset[_]): ReadableWrapper = ReadableWrapper(dataset.toDF)
  implicit def readableWrapperDF(dataFrame: DataFrame): ReadableWrapper = ReadableWrapper(dataFrame)

  // Get Layout
  implicit def getGveLayout[L <: Gve[L]](graph: BaseGraph[L]): GveLayout[L] = graph.layout
  implicit def getTflLayout[L <: Tfl[L]](graph: BaseGraph[L]): TflLayout[L] = graph.layout
}

object Implicits extends Implicits
