package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.Row
import org.gradoop.spark.io.impl.csv.CsvConstants.ParseFunction

protected class RowToObject[T](parseFunctions: Array[ParseFunction[T]]) extends Serializable {

  /** Create a object by applying each parse function on the row field at the same position.
   *
   * The object is passed (in order) as option through the functions and the final result is returned.
   * If the end result is {@link None}, nothing is returned.
   *
   * @param row row of strings as result of reading a csv file
   * @return 0 or 1 element in a traversable
   */
  def call(row: Row): Traversable[T] = {
    var element: Option[T] = None
    for (i <- 0 until row.size) {
      element = if (row.isNullAt(i)) parseFunctions(i)(element, "") else parseFunctions(i)(element, row.getAs[String](i))
    }
    element.fold[Traversable[T]](Traversable.empty)(e => Traversable(e))
  }
}
