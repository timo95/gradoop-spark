package org.gradoop.spark.util

import org.apache.spark.sql.{Column, Dataset}
import org.gradoop.common.model.api.elements.{Attributed, Edge, GraphElement, Identifiable, Labeled}
import org.gradoop.common.util.ColumnNames._

class ColumnSelector[T](val dataset: Dataset[T]) extends AnyVal {
  import org.apache.spark.sql.functions.map_keys

  def id(implicit ev: T <:< Identifiable): Column = dataset(ID)
  def labels(implicit ev: T <:< Labeled): Column = dataset(LABEL)
  def properties(implicit ev: T <:< Attributed): Column = dataset(PROPERTIES)
  def propertyKeys(implicit ev: T <:< Attributed): Column = map_keys(dataset(PROPERTIES))
  def graphIds(implicit ev: T <:< GraphElement): Column = dataset(GRAPH_IDS)
  def sourceId(implicit ev: T <:< Edge): Column = dataset(SOURCE_ID)
  def targetId(implicit ev: T <:< Edge): Column = dataset(TARGET_ID)
}
