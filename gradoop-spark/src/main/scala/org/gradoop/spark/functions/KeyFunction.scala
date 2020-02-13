package org.gradoop.spark.functions

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait KeyFunction extends Serializable {

  def name: String

  def extractKey: Column

  def addKey(dataFrame: DataFrame, column: Column): DataFrame

  def addKey(dataMap: Map[String, DataFrame], column: Column)(implicit sparkSession: SparkSession): Map[String, DataFrame]
}
