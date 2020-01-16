package org.gradoop.spark.expressions.transformation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.components.{Contained, Labeled}
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
      dataset.select(replaceColumn(dataset.columns, dataset.label, expression): _*).as[A]
    }
    transformationFunction
  }

  def addGraphId[A <: Contained](graphId: GradoopId)
    (implicit sparkSession: SparkSession, encoder: Encoder[A]): TransformationFunction[A] = {
    def transformationFunction(dataset: Dataset[A])
      (implicit sparkSession: SparkSession, encoder: Encoder[A]): Dataset[A] = {
      dataset.map((e: A) => {
        e.addGraphId(graphId)
        e
      })
    }
    transformationFunction
  }

  private def replaceColumn(allColumns: Array[String], oldColumn: Column, newExpression: Column): Seq[Column] = {
    (allColumns.toSet - oldColumn.toString).toSeq.map(col) :+ newExpression.as(oldColumn.toString)
  }
}
