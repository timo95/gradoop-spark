package org.gradoop.spark.util

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.components.Identifiable
import org.gradoop.common.util.ColumnNames

class DisplayConverter[E <: Identifiable](val dataset: Dataset[E]) {

  /** Workaround to show correct ids when printing the dataset.
   *
   * This is necessary, because 'Dataset#show' does not use 'toString' of GradoopId.
   */
  def idToString: DataFrame = {
    import org.apache.spark.sql.functions._
    val idToString = udf((id: Row) => new GradoopId(id.getAs[Array[Byte]](0)).toString)
    dataset.withColumn(ColumnNames.ID, idToString(dataset(ColumnNames.ID)))
  }

  def idsToString: DataFrame = {
    import org.apache.spark.sql.functions._
    val idToString = udf((id: Row) => new GradoopId(id.getAs[Array[Byte]](0)).toString)
    val idsToString = udf((ids: Seq[Row]) => ids.map(id => new GradoopId(id.getAs[Array[Byte]](0)).toString))
    dataset.withColumn(ColumnNames.ID, idToString(dataset(ColumnNames.ID)))
      .withColumn(ColumnNames.GRAPH_IDS, idsToString(dataset(ColumnNames.GRAPH_IDS)))
  }
}
