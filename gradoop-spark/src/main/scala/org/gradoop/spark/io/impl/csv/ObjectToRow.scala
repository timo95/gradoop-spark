package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction

class ObjectToRow[T](composeFunctions: Array[ComposeFunction[T]]) extends Serializable {

  /** Create a row of strings by evaluating the object with each compose function.
   *
   * @param obj object used to compose the strings
   * @return row of strings
   */
  def call(obj: T): Row = Row.fromSeq(composeFunctions.map(f => f(obj)))

  def encoder: ExpressionEncoder[Row] = {
    val fields: Seq[StructField] = composeFunctions.indices
      .map(i => StructField(i.toString, DataTypes.StringType, nullable = false))
    RowEncoder(StructType(fields))
  }
}
