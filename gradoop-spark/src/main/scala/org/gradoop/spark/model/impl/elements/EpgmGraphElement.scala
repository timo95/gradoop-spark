package org.gradoop.spark.model.impl.elements

import org.gradoop.common.model.api.elements.GraphElement

abstract class EpgmGraphElement(id: Id, labels: Labels, properties: Properties, graphIds: IdSet)
  extends EpgmElement(id, labels, properties) with GraphElement {
  override def getGraphIds: IdSet = graphIds
}
