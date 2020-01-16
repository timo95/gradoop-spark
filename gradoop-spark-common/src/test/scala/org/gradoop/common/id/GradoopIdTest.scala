package org.gradoop.common.id

import org.gradoop.common.GradoopSparkCommonTestBase
import org.scalatest.prop.TableDrivenPropertyChecks

class GradoopIdTest extends GradoopSparkCommonTestBase with TableDrivenPropertyChecks {

  describe("GradoopId") {

    it("test isValid") {
      val gradoopIds = Table(
        ("string", "valid"),
        ("912345678910111213141516", true),
        ("1AB363914FD1325CC43790AB", true),
        ("bcdef12345678910bac76d4e", true),
        ("abc3451d98ebd3452fff32a", false), // too short
        ("1234567891011121131415161", false), // too long
        ("12345678910111211314151G", false) // 'G' is not a valid char in a hex string
      )

      forEvery(gradoopIds) { (string, valid) =>
        assert(GradoopId.isValid(string) == valid)
      }
    }
  }
}
