package org.gradoop.spark.model.impl.operators.verify

class RemoveDanglingEdgesTest extends RemoveDanglingEdgesBehaviors {

  describe("GveRemoveDanglingEdges") {
    it should behave like removeDanglingEdges(_.removeDanglingEdges)
  }

  describe("TflRemoveDanglingEdges") {
    it should behave like removeDanglingEdges(_.asTfl(tflConfig).removeDanglingEdges.asGve(gveConfig))
  }
}
