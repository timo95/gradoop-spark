package org.gradoop.spark.model.impl.operators.grouping.gve

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.model.impl.operators.grouping.Functions.{DEFAULT_AGG, longToId}

private[gve] object Functions {

  /** Transforms the given columns to a property map.
   *
   * Any previous property map is overwritten.
   * If the column list is empty, an empty property map is added.
   *
   * @param dataFrame dataframe with given columns
   * @param columnNames names used for columns
   * @return dataframe with given columns moved to properties map
   */
  def columnsToProperties(dataFrame: DataFrame, columnNames: Seq[String]): DataFrame = {
    if(columnNames.isEmpty) {
      dataFrame.withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(DEFAULT_AGG.name)
    } else {
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
