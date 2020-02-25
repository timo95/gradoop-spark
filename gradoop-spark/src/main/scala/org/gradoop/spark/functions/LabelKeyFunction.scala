package org.gradoop.spark.functions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.gradoop.common.util.ColumnNames

class LabelKeyFunction extends KeyFunction {

  /* Used to improve performance with tfl layout */
  var labelOpt: Option[Array[String]] = None

  override def name: String = ":label"

  override def extractKey: Column = col(ColumnNames.LABEL)

  override def addKey(dataFrame: DataFrame, column: Column): DataFrame = {
    dataFrame.withColumn(ColumnNames.LABEL, column)
  }

  override def addKey(dataMap: Map[String, DataFrame], column: Column)
    (implicit sparkSession: SparkSession): Map[String, DataFrame] = {
    import sparkSession.implicits._
    dataMap.flatMap(e => {
      val df = if(labelOpt.isDefined) e._2 else e._2.cache.toDF
      val labels = labelOpt.getOrElse(df.select(column).distinct.as[String].collect)
      labels.map(l => (l, addKey(df, column).filter(col(ColumnNames.LABEL) === lit(l)).toDF)).toTraversable
    })
  }
}

object LabelKeyFunction {
  def apply(): LabelKeyFunction = new LabelKeyFunction()
}
