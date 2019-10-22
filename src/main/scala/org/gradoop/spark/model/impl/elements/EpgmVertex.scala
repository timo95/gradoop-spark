package org.gradoop.spark.model.impl.elements

import org.gradoop.spark.model.api.elements.Vertex

class EpgmVertex(id: Id, labels: Labels, properties: Properties, graphIds: IdSet) extends EpgmGraphElement(id, labels, properties, graphIds) with Vertex {

}
