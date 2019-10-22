package org.gradoop.spark.model.impl.elements

import org.gradoop.spark.model.api.elements.GraphElement

import org.gradoop.spark.model.api.elements._

abstract class EpgmGraphElement(id: Id, labels: Labels, properties: Properties, graphIds: IdSet) extends EpgmElement(id, labels, properties) with GraphElement {
  override def getGraphIds: IdSet = graphIds
}
