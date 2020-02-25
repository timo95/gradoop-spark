package org.gradoop.spark.functions

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait KeyFunction extends Serializable {

  /** This name is used as row name internally.
   * Should not be the same as element fields or other key functions.
   *
   * @return name
   */
  def name: String

  /** Column expression to extract the grouping key from the dataframe.
   *
   * @return column expression
   */
  def extractKey: Column

  /** Add key to resulting super element (or do nothing).
   *
   * @param dataFrame dataframe to add key to
   * @param column temporary column containing the key
   * @return dataFrame with key added to some element column
   */
  def addKey(dataFrame: DataFrame, column: Column): DataFrame

  /** Tfl version of #addKey */
  def addKey(dataMap: Map[String, DataFrame], column: Column)(implicit sparkSession: SparkSession): Map[String, DataFrame]
}
