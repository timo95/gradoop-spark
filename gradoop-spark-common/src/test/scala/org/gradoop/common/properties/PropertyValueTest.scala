package org.gradoop.common.properties

import org.gradoop.common.GradoopSparkCommonTestBase
import org.gradoop.common.model.impl.id.GradoopId
import org.scalatest.prop.TableDrivenPropertyChecks

class PropertyValueTest extends GradoopSparkCommonTestBase with TableDrivenPropertyChecks {

  val values = Table(
    ("type", "value"),
    (Type.NULL, null),
    (Type.BOOLEAN, true),
    (Type.SHORT, 23.asInstanceOf[Short]),
    (Type.INTEGER, 23),
    (Type.LONG, 23L),
    (Type.FLOAT, 2.3f),
    (Type.DOUBLE, 2.3d),
    (Type.STRING, "23"),
    (Type.BIG_DECIMAL, BigDecimal(23)),
    (Type.GRADOOP_ID, GradoopId.get)
  )


  forAll(values) { (typ, value) =>
    describe("PropertyValue (%s)".format(typ.string)) {
      val propertyValue = PropertyValue(value)
      it("Returns correct type") {
        assert(propertyValue.getType equals typ)
      }
      it("Returns correct value") {
        assert(propertyValue.get == value)
      }

      val copy = propertyValue.copy
      it("Correctly copy value") {
        assert(propertyValue.get == copy.get)
      }
      if (value != null && !typ.getTypeClass.isPrimitive) {
        it("Returns do not copy reference") {
          assert(propertyValue.get.asInstanceOf[AnyRef] ne copy.get.asInstanceOf[AnyRef])
        }
      }
    }
  }
}
