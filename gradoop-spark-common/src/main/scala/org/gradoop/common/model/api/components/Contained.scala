package org.gradoop.common.model.api.components

import org.gradoop.common.id.GradoopId

trait Contained {

  def graphIds: IdSet

  def graphCount: Int = graphIds.size

  def graphIds_=(graphIds: IdSet): Unit

  def addGraphId(graphId: GradoopId): Unit = graphIds = graphIds + graphId
}
