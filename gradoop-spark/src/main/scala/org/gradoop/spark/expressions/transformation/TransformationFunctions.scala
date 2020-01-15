package org.gradoop.spark.expressions.transformation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.components.Labeled
import org.gradoop.spark.util.Implicits._

object TransformationFunctions {
  type TransformationFunction[A] = Dataset[A] => Dataset[A]

  def identity[A]: TransformationFunction[A] = (dataset: Dataset[A]) => dataset

  def renameLabel[A <: Labeled](oldLabel: String, newLabel: String)
                               (implicit sparkSession: SparkSession, encoder: Encoder[A]): TransformationFunction[A] = {
    def transformationFunction(dataset: Dataset[A])
                              (implicit sparkSession: SparkSession, encoder: Encoder[A]): Dataset[A] = {
      import sparkSession.implicits._
      val expression: Column = when(dataset.label === lit(oldLabel), lit(newLabel)).otherwise(dataset.label)
      val otherColumns: Seq[Column] = (dataset.columns.toSet - dataset.label.toString).toSeq.map(col)
      dataset.select(otherColumns :+ expression.as(dataset.label.toString): _*).as[A]
    }
    transformationFunction
  }
}
