package org.gradoop.spark.model.impl.operators.setgraph

class OverlapTest extends OverlapBehaviors {

  describe("GveOverlap") {
    it should behave like overlap((left, right) => left.overlap(right))
  }

  describe("TflOverlap") {
    it should behave like overlap((left, right) => {
      left.asTfl(tflConfig).overlap(right.asTfl(tflConfig)).asGve(left.config)
    })
  }
}
