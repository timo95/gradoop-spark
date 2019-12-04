package org.gradoop.spark.util

import org.apache.spark.sql.{Column, Dataset, Encoder, TypedColumn}
import org.gradoop.common.model.api.components._
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.util.ColumnNames._

class ColumnSelector[T](val dataset: Dataset[T]) extends AnyVal {
  import org.apache.spark.sql.functions.map_keys

  def id(implicit ev: T <:< Identifiable, encoder: Encoder[GradoopId]): Column = dataset(ID).as[GradoopId]
  def label(implicit ev: T <:< Labeled, encoder: Encoder[String]): TypedColumn[Any, String] = dataset(LABEL).as[String]
  def properties(implicit ev: T <:< Attributed, encoder: Encoder[Properties]): Column = dataset(PROPERTIES).as[Properties]
  def propertyKeys(implicit ev: T <:< Attributed, encoder: Encoder[Array[String]]): Column = map_keys(dataset(PROPERTIES)).as[Array[String]]
  def graphIds(implicit ev: T <:< GraphElement, encoder: Encoder[IdSet]): Column = dataset(GRAPH_IDS).as[IdSet]
  def sourceId(implicit ev: T <:< Edge, encoder: Encoder[GradoopId]): Column = dataset(SOURCE_ID).as[GradoopId]
  def targetId(implicit ev: T <:< Edge, encoder: Encoder[GradoopId]): Column = dataset(TARGET_ID).as[GradoopId]
}
