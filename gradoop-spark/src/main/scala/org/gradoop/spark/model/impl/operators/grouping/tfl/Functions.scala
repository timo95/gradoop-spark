package org.gradoop.spark.model.impl.operators.grouping.tfl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, lit, map_from_arrays, monotonically_increasing_id, typedLit}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.impl.operators.grouping.Functions.{DEFAULT_AGG, getAlias, longToId}

private[tfl] object Functions {

  /** Transforms the given columns to a property map.
   *
   * Any previous property map is overwritten.
   * If the column list is empty, an empty property map is added.
   *
   * @param dataMap dataframe with given columns
   * @param columns column expressions used for columns
   * @return dataframe with given columns moved to properties map
   */
  def columnsToProperties(dataMap: Map[String, DataFrame], columns: Seq[Column]): Map[String, DataFrame] = {
    if(columns.isEmpty) {
      dataMap.mapValues(_.withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(getAlias(DEFAULT_AGG)))
    } else {
      val columnNames = columns.map(c => getAlias(c))
      dataMap.mapValues(_.withColumn(ColumnNames.PROPERTIES, map_from_arrays(
        array(columnNames.map(n => lit(n)): _*),
        array(columnNames.map(n => col(n)): _*)))
        .drop(columnNames: _*))
    }
  }

  /** Adds default id, label and graphIds to dataframe.
   *
   * @param dataMap dataframe
   * @return dataframe with new id, default label and empty graph ids
   */
  def addDefaultColumns(dataMap: Map[String, DataFrame]): Map[String, DataFrame] = {
    dataMap.transform((l, df) => df.select(df("*"),
      longToId(monotonically_increasing_id()).as(ColumnNames.ID),
      lit(l).as(ColumnNames.LABEL),
      typedLit[Array[GradoopId]](Array.empty).as(ColumnNames.GRAPH_IDS)
    ))
  }
}
