package org.gradoop.spark.model.impl.operators.grouping.gve

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, lit, map_from_arrays, monotonically_increasing_id, typedLit}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.model.impl.operators.grouping.Functions.{DEFAULT_AGG, getAlias, longToId}

private[gve] object Functions {

  /** Transforms the given columns to a property map.
   *
   * Any previous property map is overwritten.
   * If the column list is empty, an empty property map is added.
   *
   * @param dataFrame dataframe with given columns
   * @param columns column expressions used for columns
   * @return dataframe with given columns moved to properties map
   */
  def columnsToProperties(dataFrame: DataFrame, columns: Seq[Column]): DataFrame = {
    if(columns.isEmpty) {
      dataFrame.withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(getAlias(DEFAULT_AGG))
    } else {
      val columnNames = columns.map(c => getAlias(c))
      dataFrame.withColumn(ColumnNames.PROPERTIES, map_from_arrays(
        array(columnNames.map(n => lit(n)): _*),
        array(columnNames.map(n => col(n)): _*)))
        .drop(columnNames: _*)
    }
  }

  /** Adds default id, label and graphIds to dataframe.
   *
   * @param dataFrame dataframe
   * @return dataframe with new id, default label and empty graph ids
   */
  def addDefaultColumns(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(dataFrame("*"),
      longToId(monotonically_increasing_id()).as(ColumnNames.ID),
      lit(GradoopConstants.DEFAULT_GRAPH_LABEL).as(ColumnNames.LABEL),
      typedLit[Array[GradoopId]](Array.empty).as(ColumnNames.GRAPH_IDS))
  }
}
