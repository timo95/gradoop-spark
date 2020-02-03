package org.gradoop.spark.model.impl.operators.difference

class DifferenceTest extends DifferenceBehaviors {

  describe("GveDifference") {
    it should behave like difference((left, right) => left.difference(right))
  }

  describe("TflDifference") {
    //it should behave like difference((left, right) => {
    // left.asTfl(tflConfig).difference(right.asTfl(tflConfig)).asGve(left.config)
    // })
  }
}
