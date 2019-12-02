package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.gradoop.spark.model.impl.types.LayoutType

trait BaseLayoutFactory[L <: LayoutType[L]] extends Serializable {

  implicit def sparkSession: SparkSession

  def createDataset[T](iterable: Iterable[T])(implicit encoder: Encoder[T]): Dataset[T] =
    sparkSession.createDataset(iterable.toSeq)
}
