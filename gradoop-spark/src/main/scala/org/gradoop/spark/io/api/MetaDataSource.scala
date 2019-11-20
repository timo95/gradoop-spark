package org.gradoop.spark.io.api

import org.gradoop.spark.io.impl.metadata.MetaData

trait MetaDataSource extends Serializable {

  def read: MetaData
}
