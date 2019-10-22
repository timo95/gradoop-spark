package org.gradoop.spark.model.impl.elements

import org.gradoop.spark.model.api.elements.GraphHead

class EpgmGraphHead(id: Id, labels: Labels, properties: Properties) extends EpgmElement(id, labels, properties) with GraphHead {

}
