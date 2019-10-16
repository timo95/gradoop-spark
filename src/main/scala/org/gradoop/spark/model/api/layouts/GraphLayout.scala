package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.DataFrame
import org.gradoop.common.model.api.entities.Element

trait GraphLayout[E <: Element] extends GraphCollectionLayout[E] {

}
