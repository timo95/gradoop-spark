package org.gradoop.spark.model.impl.operators.grouping.tfl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.model.impl.operators.grouping.Functions.{DEFAULT_AGG, longToId}

private[tfl] object Functions {

  /** Transforms the given columns to a property map.
   *
   * Any previous property map is overwritten.
   * If the column list is empty, an empty property map is added.
   *
   * @param dataMap dataframe with given columns
   * @param columnNames names used for columns
   * @return dataframe with given columns moved to properties map
   */
  def columnsToProperties(dataMap: Map[String, DataFrame], columnNames: Seq[String]): Map[String, DataFrame] = {
    if(columnNames.isEmpty) {
      dataMap.mapValues(_.withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(DEFAULT_AGG.name))
    } else {
      dataMap.mapValues(_.withColumn(ColumnNames.PROPERTIES, map_from_arrays(
        array(columnNames.map(n => lit(n)): _*),
        array(columnNames.map(n => col(n)): _*)))
        .drop(columnNames: _*))
    }
  }

  /** Adds default label and graphIds to dataframe.
   *
   * @param dataFrame dataframe
   * @return dataframe with new id, default label and empty graph ids
   */
  def addDefaultColumns(dataFrame: DataFrame): Map[String, DataFrame] = {
    Map(GradoopConstants.DEFAULT_GRAPH_LABEL -> dataFrame.transform(
      _.withColumn(ColumnNames.LABEL, lit(GradoopConstants.DEFAULT_GRAPH_LABEL))
      .withColumn(ColumnNames.GRAPH_IDS, typedLit[Array[GradoopId]](Array.empty))
    ))
  }
}
