package org.gradoop.common.properties

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.GradoopSparkCommonTestBase
import org.gradoop.common.model.impl.id.GradoopId
import org.scalatest.prop.TableDrivenPropertyChecks

class PropertyValueTest extends GradoopSparkCommonTestBase with TableDrivenPropertyChecks {

  val values = Table(
    ("name", "type", "value"),
    ("null", Type.NULL, null),
    ("boolean", Type.BOOLEAN, true),
    ("short", Type.SHORT, 23.asInstanceOf[Short]),
    ("int", Type.INTEGER, 23),
    ("long", Type.LONG, 23L),
    ("float", Type.FLOAT, 2.3f),
    ("double", Type.DOUBLE, 2.3d),
    ("string", Type.STRING, "23"),
    ("bigdecimal", Type.BIG_DECIMAL, BigDecimal(23)),
    ("gradoopid", Type.GRADOOP_ID, GradoopId.get),
    ("date", Type.DATE, LocalDate.now),
    ("time", Type.TIME, LocalTime.now),
    ("datetime", Type.DATE_TIME, LocalDateTime.now),
    ("list", Type.LIST, List[PropertyValue](PropertyValue(23), PropertyValue(22))),
    ("set", Type.SET, Set[PropertyValue](PropertyValue(23), PropertyValue(22))),
    ("map", Type.MAP, Map[PropertyValue, PropertyValue](PropertyValue("key") -> PropertyValue("value"))),
    ("empty list", Type.LIST, List.empty[PropertyValue]),
    ("empty set", Type.SET, Set.empty[PropertyValue]),
    ("empty map", Type.MAP, Map.empty[PropertyValue, PropertyValue])
  )

  describe("PropertyValue") {

    forEvery(values) { (name, typ, value) =>
      describe(name) {
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
        val nonEmpty = !value.isInstanceOf[Iterable[_]] || value.asInstanceOf[Iterable[_]].nonEmpty
        if (value != null && !typ.getTypeClass.isPrimitive && nonEmpty) {
          it("Copy has different reference") {
            assert(propertyValue.get.asInstanceOf[AnyRef] ne copy.get.asInstanceOf[AnyRef])
          }
        }
      }
    }

    it("Exception for unsupported types") {
      assertThrows[IllegalArgumentException](PropertyValue(PropertyValue.NULL_VALUE))
    }
    it("Exception for unsupported inner types") {
      assertThrows[IllegalArgumentException](PropertyValue(List("el1", "el2")))
      assertThrows[IllegalArgumentException](PropertyValue(Set("el3", "el4")))
      assertThrows[IllegalArgumentException](PropertyValue(Map("key" -> "el")))
    }
  }
}
