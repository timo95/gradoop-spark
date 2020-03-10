package org.gradoop.spark.model.api.config

import org.apache.spark.sql._
import org.gradoop.common.model.api.components.ComponentTypes
import org.gradoop.spark.model.impl.types.LayoutType
import org.gradoop.spark.util.Implicits

class GradoopSparkConfig[L <: LayoutType[L]](val logicalGraphFactory: L#LGF, val graphCollectionFactory: L#GCF)
(implicit val sparkSession: SparkSession) extends Serializable {

  object Implicits extends SQLImplicits with Implicits with ComponentTypes {
    protected override def _sqlContext: SQLContext = sparkSession.sqlContext

    // Spark session
    implicit val implicitSparkSession = sparkSession
  }
}
