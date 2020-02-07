package org.gradoop.spark.util

import org.apache.spark.sql.{DataFrame, Row}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames

class DisplayConverter(val dataFrame: DataFrame) {

  def readable: DataFrame = {
    import org.apache.spark.sql.functions._
    val idToString = udf((id: Row) => new GradoopId(id.getAs[Array[Byte]](0)).toString)
    val idsToString = udf((ids: Seq[Row]) => {
      ids.map(id => new GradoopId(id.getAs[Array[Byte]](0)).toString)
    })
    val propToString = udf((props: Seq[Row]) => {
      props.map(prop => new PropertyValue(prop(0).asInstanceOf[Array[Byte]]).toString)
    })

    val cols = dataFrame.columns

    var result = dataFrame
    if(cols.contains(ColumnNames.ID)) result = result
      .withColumn(ColumnNames.ID, idToString(dataFrame(ColumnNames.ID)))
    if(cols.contains("superId")) result = result
      .withColumn("superId", idToString(dataFrame("superId")))
    if(cols.contains("vertexId")) result = result
      .withColumn("vertexId", idToString(dataFrame("vertexId")))
    if(cols.contains(ColumnNames.GRAPH_IDS)) result = result
      .withColumn(ColumnNames.GRAPH_IDS, idsToString(dataFrame(ColumnNames.GRAPH_IDS)))
    if(cols.contains(ColumnNames.SOURCE_ID)) result = result
      .withColumn(ColumnNames.SOURCE_ID, idToString(dataFrame(ColumnNames.SOURCE_ID)))
    if(cols.contains(ColumnNames.TARGET_ID)) result = result
      .withColumn(ColumnNames.TARGET_ID, idToString(dataFrame(ColumnNames.TARGET_ID)))
    if(cols.contains(ColumnNames.PROPERTIES)) result = result
      .withColumn(ColumnNames.PROPERTIES, map_from_arrays(map_keys(dataFrame(ColumnNames.PROPERTIES)),
        propToString(map_values(dataFrame(ColumnNames.PROPERTIES)))))

    result
  }
}
