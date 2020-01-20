package org.gradoop.spark.io.api

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.io.impl.metadata.MetaData

trait MetaDataSink extends Serializable {

  def write(metaData: MetaData, saveMode: SaveMode)
}
