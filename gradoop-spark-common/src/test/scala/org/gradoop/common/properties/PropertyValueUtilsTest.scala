package org.gradoop.common.properties

import org.gradoop.common.GradoopSparkCommonTestBase
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.prop.TableDrivenPropertyChecks

class PropertyValueUtilsTest extends GradoopSparkCommonTestBase with TableDrivenPropertyChecks {

  val addValues = Table(
    ("left", "right", "result"),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(13.asInstanceOf[Short]), PropertyValue(36)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(13), PropertyValue(36)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(13L), PropertyValue(36L)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(13.3f), PropertyValue(36.3f)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(13.3d), PropertyValue(36.3d)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(BigDecimal(13.3)), PropertyValue(BigDecimal(36.3))),
    (PropertyValue(23), PropertyValue(13), PropertyValue(36)),
    (PropertyValue(23), PropertyValue(13L), PropertyValue(36L)),
    (PropertyValue(23), PropertyValue(13.3f), PropertyValue(36.3f)),
    (PropertyValue(23), PropertyValue(13.3d), PropertyValue(36.3d)),
    (PropertyValue(23), PropertyValue(BigDecimal(13.3)), PropertyValue(BigDecimal(36.3))),
    (PropertyValue(23L), PropertyValue(13L), PropertyValue(36L)),
    (PropertyValue(23L), PropertyValue(13.3f), PropertyValue(36.3f)),
    (PropertyValue(23L), PropertyValue(13.3d), PropertyValue(36.3d)),
    (PropertyValue(23L), PropertyValue(BigDecimal(13.3)), PropertyValue(BigDecimal(36.3))),
    (PropertyValue(23.3f), PropertyValue(13.3f), PropertyValue(36.6f)),
    (PropertyValue(23.3f), PropertyValue(13.3d), PropertyValue(36.6d)),
    (PropertyValue(23.3f), PropertyValue(BigDecimal(13.3)), PropertyValue(BigDecimal(36.6))),
    (PropertyValue(23.3d), PropertyValue(13.3d), PropertyValue(36.6d)),
    (PropertyValue(23.3d), PropertyValue(BigDecimal(13.3)), PropertyValue(BigDecimal(36.6))),
    (PropertyValue(BigDecimal(23.3)), PropertyValue(BigDecimal(13.3)), PropertyValue(BigDecimal(36.6)))
  )

  val multiplyValues = Table(
    ("left", "right", "result"),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(3.asInstanceOf[Short]), PropertyValue(69)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(3), PropertyValue(69)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(3L), PropertyValue(69L)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(3.3f), PropertyValue(75.9f)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(3.3d), PropertyValue(75.9d)),
    (PropertyValue(23.asInstanceOf[Short]), PropertyValue(BigDecimal(3.3)), PropertyValue(BigDecimal(75.9))),
    (PropertyValue(23), PropertyValue(3), PropertyValue(69)),
    (PropertyValue(23), PropertyValue(3L), PropertyValue(69L)),
    (PropertyValue(23), PropertyValue(3.3f), PropertyValue(75.9f)),
    (PropertyValue(23), PropertyValue(3.3d), PropertyValue(75.9d)),
    (PropertyValue(23), PropertyValue(BigDecimal(3.3)), PropertyValue(BigDecimal(75.9))),
    (PropertyValue(23L), PropertyValue(3L), PropertyValue(69L)),
    (PropertyValue(23L), PropertyValue(3.3f), PropertyValue(75.9f)),
    (PropertyValue(23L), PropertyValue(3.3d), PropertyValue(75.9d)),
    (PropertyValue(23L), PropertyValue(BigDecimal(3.3)), PropertyValue(BigDecimal(75.9))),
    (PropertyValue(23.3f), PropertyValue(3.3f), PropertyValue(76.89f)),
    (PropertyValue(23.3f), PropertyValue(3.3d), PropertyValue(76.89d)),
    (PropertyValue(23.3f), PropertyValue(BigDecimal(3.3)), PropertyValue(BigDecimal(76.89))),
    (PropertyValue(23.3d), PropertyValue(3.3d), PropertyValue(76.89d)),
    (PropertyValue(23.3d), PropertyValue(BigDecimal(3.3)), PropertyValue(BigDecimal(76.89))),
    (PropertyValue(BigDecimal(23.3)), PropertyValue(BigDecimal(3.3)), PropertyValue(BigDecimal(76.89)))
  )

  describe("Add") {
    it("Correct type") {
      forEvery(addValues) { (left, right, result) =>
        assert(PropertyValueUtils.add(left, right).getType == result.getType)
        assert(PropertyValueUtils.add(right, left).getType == result.getType)
      }
    }

    it("Correct value") {
      forEvery(addValues) { (left, right, result) =>
        implicit val tolerant: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-4d)
        assert(PropertyValueUtils.add(left, right).getNumber.doubleValue === result.getNumber.doubleValue)
        assert(PropertyValueUtils.add(right, left).getNumber.doubleValue === result.getNumber.doubleValue)
      }
    }
  }

  describe("Multiply") {
    it("Correct type") {
      forEvery(multiplyValues) { (left, right, result) =>
        assert(PropertyValueUtils.multiply(left, right).getType == result.getType)
        assert(PropertyValueUtils.multiply(right, left).getType == result.getType)
      }
    }

    it("Correct value") {
      forEvery(multiplyValues) { (left, right, result) =>
        implicit val tolerant: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-4d)
        assert(PropertyValueUtils.multiply(left, right).getNumber.doubleValue === result.getNumber.doubleValue)
        assert(PropertyValueUtils.multiply(right, left).getNumber.doubleValue === result.getNumber.doubleValue)
      }
    }
  }

  describe("Max") {
    it("Correct value") {
      forEvery(addValues) { (higher, lower, _) =>
        implicit val tolerant: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-4d)
        assert(PropertyValueUtils.max(higher, lower) == higher)
        assert(PropertyValueUtils.max(lower, higher) == higher)
      }
    }
  }

  describe("Min") {
    it("Correct value") {
      forEvery(addValues) { (higher, lower, _) =>
        implicit val tolerant: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-4d)
        assert(PropertyValueUtils.min(higher, lower) == lower)
        assert(PropertyValueUtils.min(lower, higher) == lower)
      }
    }
  }
}
