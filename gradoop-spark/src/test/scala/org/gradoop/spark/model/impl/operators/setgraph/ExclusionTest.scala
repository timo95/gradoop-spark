package org.gradoop.spark.model.impl.operators.setgraph

class ExclusionTest extends ExclusionBehaviors {

  describe("GveExclusion") {
    it should behave like exclusion((left, right) => left.exclude(right))
  }

  describe("TflExclusion") {
    it should behave like exclusion((left, right) => {
      left.asTfl(tflConfig).exclude(right.asTfl(tflConfig)).asGve(left.config)
    })
  }
}
