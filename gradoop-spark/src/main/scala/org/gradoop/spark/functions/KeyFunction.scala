package org.gradoop.spark.functions

import org.apache.spark.sql.{Column, DataFrame}

trait KeyFunction extends Serializable {

  def name: String

  def extractKey: Column

  def addKeyToElement(dataFrame: DataFrame, column: Column): DataFrame
}
