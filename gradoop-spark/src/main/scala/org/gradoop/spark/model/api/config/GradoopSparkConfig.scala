package org.gradoop.spark.model.api.config

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.components.ComponentTypes
import org.gradoop.spark.model.impl.types.LayoutType
import org.gradoop.spark.util.Implicits

class GradoopSparkConfig[L <: LayoutType[L]](val logicalGraphFactory: L#LGF, val graphCollectionFactory: L#GCF)
(implicit val sparkSession: SparkSession) extends Serializable {

  object Implicits extends Implicits with ComponentTypes {
    // Spark session
    implicit val implicitSparkSession = sparkSession
  }
}
