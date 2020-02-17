package org.gradoop.spark.util

import org.apache.spark.sql.functions.map_keys
import org.apache.spark.sql.{Dataset, Encoder, TypedColumn}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.components._
import org.gradoop.common.util.ColumnNames._

trait ImplicitTypedColumnSelector extends Serializable {

  implicit class IdentifiableSelector[T <: Identifiable](val dataset: Dataset[T]) {
    def id(implicit encoder: Encoder[GradoopId]): TypedColumn[Any, GradoopId] = dataset(ID).as[GradoopId]
  }

  implicit class LabeledSelector[T <: Labeled](val dataset: Dataset[T]) {
    def label(implicit encoder: Encoder[String]): TypedColumn[Any, String] = dataset(LABEL).as[String]
  }

  implicit class AttributedSelector[T <: Attributed](val dataset: Dataset[T]) {
    def properties(implicit encoder: Encoder[Properties]): TypedColumn[Any, Properties] =
      dataset(PROPERTIES).as[Properties]
    def propertyKeys(implicit encoder: Encoder[Array[String]]): TypedColumn[Any, Array[String]] =
      map_keys(dataset(PROPERTIES)).as[Array[String]]
  }

  implicit class ContainedSelector[T <: Contained](val dataset: Dataset[T]) {
    def graphIds(implicit encoder: Encoder[IdSet]): TypedColumn[Any, IdSet] = dataset(GRAPH_IDS).as[IdSet]
  }

  implicit class EdgeSelector[T <: Edge](val dataset: Dataset[T]) {
    def sourceId(implicit encoder: Encoder[GradoopId]): TypedColumn[Any, GradoopId] = dataset(SOURCE_ID).as[GradoopId]
    def targetId(implicit encoder: Encoder[GradoopId]): TypedColumn[Any, GradoopId] = dataset(TARGET_ID).as[GradoopId]
  }
}
