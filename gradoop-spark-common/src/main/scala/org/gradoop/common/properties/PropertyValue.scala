package org.gradoop.common.properties

import scala.reflect.api.TypeTags
import scala.reflect.runtime.universe._

case class PropertyValue(value: Array[Byte], typeTag: String) extends Serializable {

  def getString: String = value.toString
}

object PropertyValue {

  private def createPropertyValue(bytes: Array[Byte], typeTag: TypeTag[_]): PropertyValue = {
    new PropertyValue(bytes, typeTag.toString())
  }

  def apply(value: String): PropertyValue = createPropertyValue(value.getBytes, typeTag[String])

  def apply(value: Int): PropertyValue = createPropertyValue(Array(value.toByte), typeTag[Int])

  def apply(value: Double): PropertyValue = createPropertyValue(Array(value.toByte), typeTag[Double])

}
