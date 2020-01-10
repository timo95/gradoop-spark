package org.gradoop.common.properties

import java.time.{LocalDate, LocalDateTime, LocalTime}

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
    (Type.GRADOOP_ID, GradoopId.get),
    (Type.DATE, LocalDate.now),
    (Type.TIME, LocalTime.now),
    (Type.DATE_TIME, LocalDateTime.now),
    (Type.MAP, Map[PropertyValue, PropertyValue](PropertyValue("key") -> PropertyValue("value"))),
    (Type.LIST, List[PropertyValue](PropertyValue(true), PropertyValue("element"))),
    (Type.SET, Set[PropertyValue](PropertyValue(23)))
  )

  forAll(values) { (typ, value) =>
    describe("PropertyValue (%s)".format(typ.string)) {
      val propertyValue = PropertyValue(value)
      it("Returns correct type") {
        assert(propertyValue.getType equals typ)
      }
      it("Returns correct value") {
        val prop = propertyValue.get
        prop match {
          case null => assert(value == null)
          case iterable: Iterable[_] =>
            assert(iterable.sameElements(value.asInstanceOf[Iterable[_]]))
          case any: Any => assert(any.equals(value))
        }
      }

      val copy = propertyValue.copy
      it("Correctly copy value") {
        assert(propertyValue.get == copy.get)
      }
      if (value != null && !typ.getTypeClass.isPrimitive) {
        it("Copy has different reference") {
          assert(propertyValue.get.asInstanceOf[AnyRef] ne copy.get.asInstanceOf[AnyRef])
        }
      }
    }
  }
}
