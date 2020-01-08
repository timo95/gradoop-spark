package org.gradoop.common.properties

import java.io.{DataInputStream, OutputStream}
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.strategies.PropertyValueStrategyFactory

case class PropertyValue(value: Array[Byte]) {

  def copy: PropertyValue = PropertyValue(value)

  def getTypeByte: Byte = value(0)

  def getType: Type = Type(getTypeByte)

  def getExactType: Type = PropertyValueStrategyFactory.get(getTypeByte).getExactType(value)

  def get: Any = PropertyValueStrategyFactory.get(getTypeByte).get(value)

  def write(outputStream: OutputStream): Unit = {
    outputStream.write(value)
  }

  override def toString: String = s"${get.toString}:${getExactType.string}"

  // ---------- Convenience accessors ----------
  // Get primitive types
  def getBoolean: Boolean = get.ensuring(_.isInstanceOf[Boolean]).asInstanceOf[Boolean]
  def getShort: Short = get.ensuring(_.isInstanceOf[Short]).asInstanceOf[Short]
  def getInt: Int = get.ensuring(_.isInstanceOf[Int]).asInstanceOf[Int]
  def getLong: Long = get.ensuring(_.isInstanceOf[Long]).asInstanceOf[Long]
  def getFloat: Float = get.ensuring(_.isInstanceOf[Float]).asInstanceOf[Float]
  def getDouble: Double = get.ensuring(_.isInstanceOf[Double]).asInstanceOf[Double]
  def getString: String = get.ensuring(_.isInstanceOf[String]).asInstanceOf[String]
  def getBigDecimal: BigDecimal = get.ensuring(_.isInstanceOf[BigDecimal]).asInstanceOf[BigDecimal]
  def getGradoopId: GradoopId = get.ensuring(_.isInstanceOf[GradoopId]).asInstanceOf[GradoopId]
  def getDate: LocalDate = get.ensuring(_.isInstanceOf[LocalDate]).asInstanceOf[LocalDate]
  def getTime: LocalTime = get.ensuring(_.isInstanceOf[LocalTime]).asInstanceOf[LocalTime]
  def getDateTime: LocalDateTime = get.ensuring(_.isInstanceOf[LocalDateTime]).asInstanceOf[LocalDateTime]

  // Get compound types
  def getList: List[PropertyValue] = get.ensuring(_.isInstanceOf[List[_]]).asInstanceOf[List[PropertyValue]]
  def getSet: Set[PropertyValue] = get.ensuring(_.isInstanceOf[Set[_]]).asInstanceOf[Set[PropertyValue]]
  def getMap: Map[PropertyValue, PropertyValue] = get.ensuring(_.isInstanceOf[Map[_, _]])
    .asInstanceOf[Map[PropertyValue, PropertyValue]]
}

object PropertyValue {

  /** Value offset in byte */
  val OFFSET: Byte = 0x01.toByte

  /** Bit flag indicating a "large" property. The length of the byte representation will be stored as an {@code int} instead. */
  val FLAG_LARGE: Byte = 0x80.toByte

  /** If the length of the byte representation is larger than this value, FLAG_LARGE will be set. */
  val LARGE_PROPERTY_THRESHOLD: Int = Short.MaxValue

  val NULL_VALUE = new PropertyValue(Array(Type.NULL.byte))

  def apply(value: Any): PropertyValue = new PropertyValue(PropertyValueStrategyFactory.getBytes(value))

  def read(inputStream: DataInputStream): PropertyValue = {
    val typeByte = inputStream.readByte
    // Apply bitmask to get the actual type.
    val strategy = PropertyValueStrategyFactory
      .get((~PropertyValue.FLAG_LARGE & typeByte).toByte)

    if (strategy == null) throw new IllegalArgumentException("No strategy for type byte from input view found")
    else new PropertyValue(strategy.readBytes(inputStream, typeByte))
  }
}
