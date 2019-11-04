package org.gradoop.spark.util

import org.gradoop.spark.EpgmGradoopSparkTestBase

class StringEscaperTest extends EpgmGradoopSparkTestBase {

  describe("split") {
    describe("empty string") {
      val splitString = StringEscaper.split("", ",")
      it("should have one empty string") {
        assert(splitString.length == 1)
        assert(splitString(0) == "")
      }
    }

    describe("only delimiter") {
      val splitString = StringEscaper.split(",", ",")
      it("should have two empty strings") {
        assert(splitString.length == 2)
        assert(splitString(0) == "")
        assert(splitString(1) == "")
      }
    }
  }
}
