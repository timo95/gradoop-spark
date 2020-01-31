package org.gradoop.spark.io.impl.metadata

class MetaDataTest extends MetaDataBehaviors {

  describe("MetaData from Gve") {
    it should behave like createMetaData(lg => MetaData(lg))
  }

  describe("MetaData from Tfl") {
    it should behave like createMetaData(lg => MetaData(lg.asTfl(tflConfig)))
  }
}
