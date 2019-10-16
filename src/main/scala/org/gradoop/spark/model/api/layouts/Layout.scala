package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.DataFrame
import org.gradoop.common.model.api.entities.Element

trait Layout[E <: Element] extends Serializable {
}
