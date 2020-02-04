package org.gradoop.spark.model.impl.operators.set

class UnionTest extends UnionBehaviors {

  describe("GveUnion") {
    it should behave like union((left, right) => left.union(right))
  }

  describe("TflUnion") {
    it should behave like union((left, right) => {
      left.asTfl(tflConfig).union(right.asTfl(tflConfig)).asGve(left.config)
    })
  }
}
