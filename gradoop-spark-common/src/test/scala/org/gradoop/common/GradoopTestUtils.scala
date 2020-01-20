package org.gradoop.common

import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue

object GradoopTestUtils {

  val SOCIAL_NETWORK_GDL_FILE = "/data/gdl/social_network.gdl"

  val PROPERTIES: Properties = getProperties


  private def getProperties: Properties = {
    val localDate = LocalDate.of(2018, 6, 1)
    val localTime = LocalTime.of(18, 6, 1)
    val stringValue1 = PropertyValue("myString1")
    val stringValue2 = PropertyValue("myString2")
    val stringList = Seq(stringValue1, stringValue2)
    val intList = Seq(PropertyValue(1234), PropertyValue(5678))
    val objectMap = Map(stringValue1 -> PropertyValue(12.345), stringValue2 -> PropertyValue(67.89))
    val stringSet = Set(stringValue1, stringValue2)
    val intSet = Set(PropertyValue(1234), PropertyValue(5678))

    Seq(
      ("key0", true),
      ("key1", 23),
      ("key2", 23L),
      ("key3", 2.3f),
      ("key4", 2.3),
      ("key5", "23"),
      ("key6", GradoopId.fromString("000000000000000000000001")),
      ("key7", localDate),
      ("key8", localTime),
      ("key9", LocalDateTime.of(localDate, localTime)),
      ("keya", BigDecimal(23)),
      ("keyb", objectMap),
      ("keyc", stringList),
      ("keyd", intList),
      ("keye", 23.asInstanceOf[Short]),
      ("keyf", null),
      ("keyg", stringSet),
      ("keyh", intSet)
    ).toMap.mapValues(v => PropertyValue(v))
  }
}
