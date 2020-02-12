package org.gradoop.spark.functions

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.gradoop.common.util.ColumnNames

class LabelKeyFunction extends KeyFunction {

  override def name: String = ":label"

  override def extractKey: Column = col(ColumnNames.LABEL)

  override def addKey(dataFrame: DataFrame, column: Column): DataFrame = {
    dataFrame.withColumn(ColumnNames.LABEL, column)
  }
}
