package org.gradoop.spark.model.impl.operators.verify

class VerifyTest extends VerifyBehaviors {

  describe("GveVerify") {
    it should behave like verify(_.verify)
  }

  describe("TflVerify") {
    it should behave like verify(_.asTfl(tflConfig).verify.asGve(gveConfig))
  }
}
