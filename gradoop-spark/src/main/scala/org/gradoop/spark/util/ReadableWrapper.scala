package org.gradoop.spark.util

import org.apache.spark.sql.{DataFrame, Row}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames

class ReadableWrapper(val dataFrame: DataFrame) {

  private val idCols = Seq(ColumnNames.ID, ColumnNames.SOURCE_ID, ColumnNames.TARGET_ID, "superId", "vertexId")
  private val idsCols = Seq(ColumnNames.GRAPH_IDS)
  private val propCols = Seq("count", "minA", "maxA", "sumA", "minB", "maxB", "sumB")
  private val propsCols = Seq(ColumnNames.PROPERTIES)

  def readable: DataFrame = {
    import org.apache.spark.sql.functions._
    val idToString = udf((id: Row) => new GradoopId(id.getAs[Array[Byte]](0)).toString)
    val idsToString = udf((ids: Seq[Row]) => {
      ids.map(id => new GradoopId(id.getAs[Array[Byte]](0)).toString)
    })
    val propToString = udf((prop: Row) => {
      new PropertyValue(prop(0).asInstanceOf[Array[Byte]]).toString
    })
    val propsToString = udf((props: Seq[Row]) => {
      props.map(prop => new PropertyValue(prop(0).asInstanceOf[Array[Byte]]).toString)
    })

    val cols = dataFrame.columns
    var result = dataFrame

    idCols.filter(cols.contains).foreach(s => result = result.withColumn(s, idToString(col(s))))
    idsCols.filter(cols.contains).foreach(s => result = result.withColumn(s, idsToString(col(s))))
    propCols.filter(cols.contains).foreach(s => result = result.withColumn(s, propToString(col(s))))
    propsCols.filter(cols.contains).foreach(s => result = {
      result.withColumn(ColumnNames.PROPERTIES, map_from_arrays(map_keys(col(s)),
        propsToString(map_values(dataFrame(ColumnNames.PROPERTIES)))))
    })

    result
  }
}
