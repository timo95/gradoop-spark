package org.gradoop.spark.model.impl.operators.setcollection

class IntersectionTest extends IntersectionBehaviors {

  describe("GveIntersection") {
    it should behave like intersection((left, right) => left.intersect(right))
  }

  describe("TflIntersection") {
    it should behave like intersection((left, right) => {
      left.asTfl(tflConfig).intersect(right.asTfl(tflConfig)).asGve(left.config)
    })
  }
}
