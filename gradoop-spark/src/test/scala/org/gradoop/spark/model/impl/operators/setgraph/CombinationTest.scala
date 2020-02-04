package org.gradoop.spark.model.impl.operators.setgraph

class CombinationTest extends CombinationBehaviors {

  describe("GveDifference") {
    it should behave like combination((left, right) => left.combine(right))
  }

  describe("TflDifference") {
    it should behave like combination((left, right) => {
      left.asTfl(tflConfig).combine(right.asTfl(tflConfig)).asGve(left.config)
    })
  }
}
