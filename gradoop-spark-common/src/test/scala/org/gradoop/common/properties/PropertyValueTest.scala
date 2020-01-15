package org.gradoop.common.properties

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.GradoopSparkCommonTestBase
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.util.Type
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
    (Type.LIST, Seq[PropertyValue](PropertyValue(23), PropertyValue(22))),
    (Type.SET, Set[PropertyValue](PropertyValue(23), PropertyValue(22))),
    (Type.MAP, Map[PropertyValue, PropertyValue](PropertyValue("key") -> PropertyValue("value"))),
    (Type.LIST, Seq.empty[PropertyValue]),
    (Type.SET, Set.empty[PropertyValue]),
    (Type.MAP, Map.empty[PropertyValue, PropertyValue])
  )

  describe("PropertyValue") {
    it("Returns correct type") {
      forEvery(values) { (typ, value) =>
        assert(PropertyValue(value).getType equals typ)
      }
    }

    it("Returns correct value") {
      forEvery(values) { (_, value) =>
        assert(PropertyValue(value).get == value)
      }
    }

    it("Copy is equal") {
      forEvery(values) { (_, value) =>
        val propertyValue = PropertyValue(value)
        val copy = propertyValue.copy
        assert(propertyValue.get == copy.get)
        assert(propertyValue.getType equals copy.getType)
      }
    }

    it("Copy has different reference") {
      forEvery(values) { (typ, value) =>
        val nonEmpty = !value.isInstanceOf[Iterable[_]] || value.asInstanceOf[Iterable[_]].nonEmpty
        whenever(value != null && !typ.getTypeClass.isPrimitive && nonEmpty) {
          val propertyValue = PropertyValue(value)
          assert(propertyValue.get.asInstanceOf[AnyRef] ne propertyValue.copy.get.asInstanceOf[AnyRef])
        }
      }
    }

    it("Exception for unsupported types") {
      assertThrows[IllegalArgumentException](PropertyValue(PropertyValue.NULL_VALUE))
    }

    it("Exception for unsupported inner types") {
      assertThrows[IllegalArgumentException](PropertyValue(Seq("el1", "el2")))
      assertThrows[IllegalArgumentException](PropertyValue(Set("el3", "el4")))
      assertThrows[IllegalArgumentException](PropertyValue(Map("key" -> "el")))
    }
  }
}
